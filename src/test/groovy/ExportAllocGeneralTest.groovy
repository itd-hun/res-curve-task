import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

import java.text.SimpleDateFormat

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExportAllocGeneralTest {

    static def script

    @BeforeAll
    static void setup() {
        GroovyShell shell = new GroovyShell()
        script = shell.parse(new File('src/main/groovy/org/onboard/exportAllocation.groovy'))
    }

    def sdf = new SimpleDateFormat("yyyy-MM-dd")

    private Date parseDate(String dateString) {
        return sdf.parse(dateString)
    }

    @Test
    void testCountWorkingDaysForWeekdays() {
        def start = parseDate("2025-01-20") // Monday
        def end = parseDate("2025-01-22")   // Wednesday

        def result = script.countWorkingDays(start, end)

        assertEquals(3, result)  // Expect 3 working days (Monday, Tuesday, Wednesday)
    }

    @Test
    void testSkipWeekendsAndCountOnlyWeekdays() {
        def start = parseDate("2025-01-17") // Friday
        def end = parseDate("2025-01-19")   // Sunday

        def result = script.countWorkingDays(start, end)

        assertEquals(1, result) // Only Friday is a working day
    }

    @Test
    void testCountZeroWorkingDaysWhenStartIsAfterEnd() {
        def start = parseDate("2025-01-20") // Monday
        def end = parseDate("2025-01-19")   // Sunday (start after end)

        def result = script.countWorkingDays(start, end)

        assertEquals(0, result) // Expect 0 working days
    }

    @Test
    void testCountWorkingDayForSingleDayWithinWeekday() {
        def start = parseDate("2025-01-20") // Monday
        def end = parseDate("2025-01-20")   // Same day

        def result = script.countWorkingDays(start, end)

        assertEquals(1, result) // Expect 1 working day
    }

    @Test
    void testHandleWeekendAtTheStartOfRange() {
        def start = parseDate("2025-01-18") // Saturday
        def end = parseDate("2025-01-20")   // Monday

        def result = script.countWorkingDays(start, end)

        assertEquals(1, result) // Skip Saturday and Sunday, count Monday
    }
}
