package org.onboard

import com.niku.xmlserver.blob.NkCurve
import com.niku.xmlserver.core.NkTime
import de.itdesign.clarity.logging.CommonLogger
import groovy.sql.Sql
import groovy.transform.Field

import java.sql.Blob
import java.sql.Timestamp
import java.text.SimpleDateFormat

sql = new Sql(connection)
@Field CommonLogger cmnLog = new CommonLogger(this)

project = null
resource = null
fromDate = null
toDate = null
period = null
projectName = null
resourceName = null

def convertToDate(String dateString) {
    def dateFormat = new SimpleDateFormat('yyyy-MM-dd\'T\'HH:mm:ss')  // Input format with 'T'
    return dateFormat.parse(dateString)
}

def getProperPeriod(String customPeriod) {
    switch (customPeriod) {
        case "z_monthly":
            return "monthly"
        case "z_weekly":
            return "weekly"
        case "z_quarterly":
            return "quarterly"
        case "z_yearly":
            return "yearly"
        default:
            return "monthly"
    }
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
                            ID = ?
                       """
    def result = sql.firstRow(query, [project])

    return [start: result.SCHEDULE_START, finish: result.SCHEDULE_FINISH]

}

def adjustCalendarForPeriodType(Calendar calendar, String periodType, boolean isEndOfPeriod) {
    switch (periodType) {
        case 'monthly':
            if (isEndOfPeriod) {
                calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH)) // End of month
            } else {
                calendar.set(Calendar.DAY_OF_MONTH, 1) // Start of month
                calendar.add(Calendar.MONTH, 1)
            }
            break
        case 'quarterly':
            if (isEndOfPeriod) {
                calendar.add(Calendar.MONTH, 3)
                calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH)) // End of quarter
            } else {
                calendar.add(Calendar.MONTH, 3)
                calendar.set(Calendar.DAY_OF_MONTH, 1) // Start of quarter
            }
            break
        case 'yearly':
            if (isEndOfPeriod) {
                calendar.set(Calendar.MONTH, Calendar.DECEMBER)
                calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH)) // End of year
            } else {
                calendar.set(Calendar.MONTH, Calendar.JANUARY)
                calendar.set(Calendar.DAY_OF_MONTH, 1) // Start of year
                calendar.add(Calendar.YEAR, 1)
            }
            break
        case 'weekly':
            calendar.add(Calendar.DAY_OF_MONTH, 7) // For weekly, both adjustments are the same
            break
        default:
            throw new IllegalArgumentException("Invalid period type: ${periodType}")
    }
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

        adjustCalendarForPeriodType(calendar, periodType, true)

        def segmentEnd = new Timestamp(calendar.time.time)

        if (segmentEnd.after(scheduleFinish)) {
            segmentEnd = scheduleFinish
        }

        def formattedStart = segmentStart.toString()
        def formattedEnd = segmentEnd.toString()
        segments.add([start: formattedStart, end: formattedEnd])

        adjustCalendarForPeriodType(calendar, periodType, false)
    }

    return segments
}

def getCurveData(Sql sql) {

    def query = """
                        SELECT 
                            ii.NAME AS PROJECTNAME, sr.FULL_NAME AS RESOURCENAME, p.PRALLOCCURVE, p.HARD_CURVE 
                        FROM 
                            INV_INVESTMENTS ii 
                        JOIN 
                            PRTEAM p ON p.PRPROJECTID = ii.ID 
                        JOIN 
                            SRM_RESOURCES sr ON sr.ID  = p.PRRESOURCEID 
                        WHERE p.PRPROJECTID = ? AND p.PRRESOURCEID  = ?
                        """

    def curveData = sql.firstRow(query, [project, resource])

    if (!curveData) {
        cmnLog.error("No data found for the given project and resource")
        throw new Exception("No data found for the given project and resource")
    }

    if (!curveData["PRALLOCCURVE"]) {
        cmnLog.error("Soft Curve Data is not found. Check with resource")
        throw new Exception("Soft Curve Data is not found. Check with resource")
    }

    if (!curveData["HARD_CURVE"]) {
        cmnLog.error("Hard Curve Data is not found. Check with resource")
        throw new Exception("Hard Curve Data is not found. Check with resource")
    }

    projectName = curveData["PROJECTNAME"]
    resourceName = curveData["RESOURCENAME"]

    cmnLog.info "Successfully got the Curve Data"
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

def assertParameters() {

    cmnLog.info "Parameters passsed to the job: [Project: ${binding.variables.get('z_project')}, Resource: ${binding.variables.get('z_resource')}, From: ${binding.variables.get('z_from_date')}, To: ${binding.variables.get('z_to_date')}, Periods: ${binding.variables.get('z_periods')}]"

    if (binding.variables.containsKey('z_project')) {
        project = z_project
    } else {
        cmnLog.warn("Expected data for z_project, but received null.")
        throw new Exception("project shouldn't be null.")
    }

    if (binding.variables.containsKey('z_resource')) {
        resource = z_resource
    } else {
        cmnLog.warn("Expected data for z_resource, but received null.")
        throw new Exception("resource shouldn't be null.")
    }

    if (binding.variables.containsKey('z_from_date')) {
        fromDate = convertToDate(z_from_date)
    } else {
        cmnLog.warn("Expected data for z_from_date, but received null.")
    }

    if (binding.variables.containsKey('z_to_date')) {
        toDate = convertToDate(z_to_date)
    } else {
        cmnLog.warn("Expected data for z_to_date, but received null.")
    }

    if (!fromDate && toDate) {
        fromDate = toDate.minus(1)
    } else if (!toDate && fromDate) {
        toDate = fromDate.plus(1)
    } else if (!fromDate && !toDate) {
        toDate = new Date()
        fromDate = toDate.minus(1)
    }

    if (fromDate && toDate && fromDate > toDate) {
        cmnLog.error("Error: fromDate is later than toDate. fromDate: $fromDate, toDate: $toDate")
        throw new Exception("fromDate is later than toDate. fromDate: $fromDate, toDate: $toDate")
    }

    if (binding.variables.containsKey('z_periods')) {
        period = getProperPeriod('z_periods')
    } else {
        cmnLog.warn("Expected data for z_periods, but received null.")
    }

    cmnLog.info "Added Values- Project: ${project}, Resource: ${resource}, From: ${fromDate}, To: ${toDate}, Period: ${period}"

}

def writeResultToCSV(result, filePath) {
    def sdf = new SimpleDateFormat("dd MMM yyyy")

    def csvFile = new File(filePath)
    def writer = new FileWriter(csvFile)

    writer.write("Project, Resource, Segment Start, Segment Finish, Soft Allocation (PD), Hard Allocation (PD)\n")

    result.each { map ->
        def project = map.project
        def resource = map.resource
        def start = sdf.format(parseDate(map.start))
        def end = sdf.format(parseDate(map.end))
        def softPD = map.softPD
        def hardPD = map.hardPD

        writer.write("$project, $resource, $start, $end, $softPD, $hardPD\n")
    }

    writer.close()

    println "CSV file created: ${csvFile.absolutePath}"
}

def runScript() {

    cmnLog.info "Script started running and Asserting Parameters"

    assertParameters()

    cmnLog.info "Completed Assertion"

    def curves = getCurveData(sql)
    def projectDuration = getProjectDuration(sql)
    def projectSegmentsByPeriod = getProjectSegmentsByPeriod(projectDuration, period)

    def softCurve = readCurveData(curves["PRALLOCCURVE"])
    def hardCurve = readCurveData(curves["HARD_CURVE"])

    NkCurve softCurveClone = softCurve.clone() as NkCurve
    NkCurve hardCurveClone = hardCurve.clone() as NkCurve

    softCurveClone.clipSegment(fromDate, toDate)
    hardCurveClone.clipSegment(fromDate, toDate)

    def softCurveSegments = handleSegments(softCurveClone)
    def hardCurveSegments = handleSegments(hardCurveClone)

    def softCurveResult = calculateWorkingDaysInPeriods(projectSegmentsByPeriod, softCurveSegments)
    def hardCurveResult = calculateWorkingDaysInPeriods(projectSegmentsByPeriod, hardCurveSegments)

    def result = []

    softCurveResult.eachWithIndex { item1, index ->
        def item2 = hardCurveResult[index]
        result.add([project: projectName, resource: resourceName, start: item1.start, end: item1.end, softPD: item1.workingDays, hardPD: item2.workingDays])
    }

    writeResultToCSV(result, 'result.csv')
}

runScript()