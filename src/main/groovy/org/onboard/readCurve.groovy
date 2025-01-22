package org.onboard

import com.niku.xmlserver.blob.NkCurve
import com.niku.xmlserver.core.NkTime
import de.itdesign.clarity.logging.CommonLogger
import groovy.sql.Sql
import groovy.transform.Field

import java.sql.Blob
import java.sql.Timestamp
import java.text.SimpleDateFormat

@Field CommonLogger cmnLog = new CommonLogger(this)

//Hard coded data for testing
project = "PR1002"
resource = 5004048

fromDate = "2024-10-01 00:00:00"
toDate = "2025-05-01 00:00:00"

format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

startDate = format.parse(fromDate)
endDate = format.parse(toDate)

//Connect Db
def connectDb() {
    Map<String, String> configData = getDbConfig();
    Sql sql = null;
    try {
        sql = Sql.newInstance(configData.url, configData.username, configData.password, configData.driver)
        def result = sql.firstRow("SELECT 1 FROM DUAL")
        if (result) {
            return sql
        } else {
            cmnLog.info("Database connection fail.")
            return null
        }
    } catch (Exception e) {
        cmnLog.error("Error in connecting to DB: ${e.getMessage()}")
    }
    return sql
}

//Getting db configure info from properties
def getDbConfig() {
    Properties properties = new Properties()
    def propertiesFile = this.class.getResourceAsStream("/application.properties")
    properties.load(propertiesFile)
    def dbUrl = properties.getProperty("db.url")
    def dbUsername = properties.getProperty("db.username")
    def dbPassword = properties.getProperty("db.password")
    def dbDriver = properties.getProperty("db.driver")
    def dbConfigData = [url: dbUrl, username: dbUsername, password: dbPassword, driver: dbDriver]
    return dbConfigData
}

def parseDate(dateStr) {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr)
}

def parseSegment(Date date) {
    def sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy")
    return sdf.parse(date.toString())
}

def countWorkingDays(Date start, Date end) {
    int workingDays = 0
    Calendar calendar = Calendar.getInstance()
    calendar.setTime(start)

    if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY) {
        calendar.add(Calendar.DAY_OF_MONTH, 2)
    } else if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
        calendar.add(Calendar.DAY_OF_MONTH, 1)
    }

    while (!calendar.getTime().after(end)) {
        if (calendar.get(Calendar.DAY_OF_WEEK) >= Calendar.MONDAY && calendar.get(Calendar.DAY_OF_WEEK) <= Calendar.FRIDAY) {
            workingDays++
        }
        calendar.add(Calendar.DAY_OF_MONTH, 1)
    }
    return workingDays
}

def calculateWorkingDaysInPeriods(periods, segments) {
    def result = []

    periods.each { period ->
        def periodStart = parseDate(period.start)
        def periodEnd = parseDate(period.end)
        def totalWorkingDays = 0

        segments.each { segment ->
            if (segment.rate == 0.0) {
                return
            }

            def segmentStart = parseSegment(segment.start)
            def segmentEnd = parseSegment(segment.finish)

            def overlapStart = periodStart.after(segmentStart) ? periodStart : segmentStart
            def overlapEnd = periodEnd.before(segmentEnd) ? periodEnd : segmentEnd

            if (!overlapStart.after(overlapEnd)) {
                def overlapWorkingDays = countWorkingDays(overlapStart, overlapEnd)
                totalWorkingDays += overlapWorkingDays
            }
        }

        result.add([start: period.start, end: period.end, workingDays: totalWorkingDays])
    }

    return result
}

def getProjectDuration(Sql sql) {

    def query = """
                        SELECT 
                            SCHEDULE_START, SCHEDULE_FINISH 
                        FROM 
                            INV_INVESTMENTS
                        WHERE 
                            CODE = ?
                       """
    def result = sql.firstRow(query, [project])

    return [start: result.SCHEDULE_START, finish: result.SCHEDULE_FINISH]

}

def getProjectSegmentsByPeriod(projectDuration, periodType) {
    def start = new NkTime(projectDuration.start).toString().replace('T', ' ')
    def finish = new NkTime(projectDuration.finish).toString().replace('T', ' ')

    def scheduleStart = Timestamp.valueOf(start)
    def scheduleFinish = Timestamp.valueOf(finish)

    def calendar = Calendar.getInstance()

    calendar.time = scheduleStart

    def segments = []

    while (calendar.time.before(scheduleFinish)) {
        def segmentStart = new Timestamp(calendar.time.time)

        switch (periodType) {
            case 'monthly':
                // Set the end of the current month
                calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH))
                break
            case 'quarterly':
                // Set the end of the current quarter (3 months ahead)
                calendar.add(Calendar.MONTH, 3)
                calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH))
                break
            case 'yearly':
                // Set the end of the current year
                calendar.set(Calendar.MONTH, Calendar.DECEMBER)
                calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH))
                break
            case 'weekly':
                // Set the end of the current week (7 days later)
                calendar.add(Calendar.DAY_OF_MONTH, 7)
                break
            default:
                throw new IllegalArgumentException("Invalid period type: ${periodType}")
        }

        def segmentEnd = new Timestamp(calendar.time.time)

        // Ensure the segment end does not go beyond the finish date
        if (segmentEnd.after(scheduleFinish)) {
            segmentEnd = scheduleFinish
        }

        def formattedStart = segmentStart.toString()
        def formattedEnd = segmentEnd.toString()

        segments.add([start: formattedStart, end: formattedEnd])

        // Reset the calendar to the start of the next segment
        switch (periodType) {
            case 'monthly':
                calendar.add(Calendar.MONTH, 1)
                calendar.set(Calendar.DAY_OF_MONTH, 1)
                break
            case 'quarterly':
                calendar.add(Calendar.MONTH, 3)
                calendar.set(Calendar.DAY_OF_MONTH, 1)
                break
            case 'yearly':
                calendar.add(Calendar.YEAR, 1)
                calendar.set(Calendar.MONTH, Calendar.JANUARY)
                calendar.set(Calendar.DAY_OF_MONTH, 1)
                break
            case 'weekly':
                calendar.add(Calendar.DAY_OF_MONTH, 7)
                break
        }
    }

    return segments
}

def getCurveData(Sql sql) {

    def query = """
                        SELECT 
                            p.PRALLOCCURVE, p.HARD_CURVE 
                        FROM 
                            PRTEAM p
                        JOIN 
                            INV_INVESTMENTS ii ON ii.ID = p.PRPROJECTID 
                        WHERE 
                            ii.CODE = ? AND p.PRRESOURCEID = ?
                        """

    def curveData = sql.firstRow(query, [project, resource])

    return curveData
}

def readCurveData(curveData) {

    Blob curveBlob = curveData as Blob
    if (curveBlob) {
        def inputStream = curveBlob.getBinaryStream()
        NkCurve curve = new NkCurve(inputStream.bytes)
        return curve
    }
    return null
}

def handleSegments(NkCurve curve) {
    def segments = curve.segments
    def segmentsList = segments.collect { [start: it.startDate, finish: it.finishDate, rate: it.rate] }
    return segmentsList
}

def runScript() {

    def sql = connectDb()
    if (sql) {

        def curves = getCurveData(sql)
        def projectDuration = getProjectDuration(sql)
        def projectSegmentsByPeriod = getProjectSegmentsByPeriod(projectDuration, "weekly")
        def softCurve = readCurveData(curves["PRALLOCCURVE"])
        def hardCurve = readCurveData(curves["HARD_CURVE"])

        NkCurve softCurveClone = softCurve.clone() as NkCurve
        softCurveClone.clipSegment(startDate, endDate)

        def softCurveSegments = handleSegments(softCurveClone)
        def result = calculateWorkingDaysInPeriods(projectSegmentsByPeriod, softCurveSegments)

        result.each { res ->
            println "Period: ${res.start} to ${res.end}, Working Days: ${res.workingDays}"
        }
    }

}

runScript()