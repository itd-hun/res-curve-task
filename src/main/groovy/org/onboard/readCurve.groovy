package org.onboard

import com.niku.xmlserver.blob.NkCurve
import com.niku.xmlserver.blob.NkSegment
import de.itdesign.clarity.logging.CommonLogger
import groovy.sql.Sql
import groovy.transform.Field

import java.sql.Blob

@Field CommonLogger cmnLog = new CommonLogger(this)

//Hard coded data for testing
project = "PR1002"
resource = 5004048

enum period {
    weeks, months, quarters, years
}

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
        def segments = curve.segments
        segments.each { NkSegment segment ->
            println("Segment - Start Date: ${segment.startDate}, Finish Date: ${segment.finishDate}, Rate: ${segment.rate}, Sum: ${segment.sum}")
        }
    }
}

def runScript() {
    def sql = connectDb()
    if (sql) {
        def curves = getCurveData(sql)
        def softCurve = readCurveData(curves["PRALLOCCURVE"])
        def hardCurve = readCurveData(curves["HARD_CURVE"])
    }
}

runScript()