package Illumination.operators;

import Illumination.models.WindowedIllumination;

import java.util.Calendar;
import java.util.Deque;
import java.util.Locale;

public class SensorAndTimeCalculator {
    private static int WorkStartTime = 8 - 1;
    private static int WorkEndTime = 17 + 1;

    /**
     * 计算当前元素是否需要报警，并维护历史队列
     *
     * @param deque
     * @param item
     */
    public static void Calculate(Deque<WindowedIllumination> deque, WindowedIllumination item) {
        //补全当前数据，存在上个窗口且上个窗口时间不长于1小时的时候，以上个窗口的时间补全
        WindowedIllumination last = deque.peekLast();
        if (deque.size() > 0 && last.TimeStamp + 60 * 60 * 1000 >= item.TimeStamp) {
            if (last.TimeStamp + 20 * 60 * 1000 < item.TimeStamp) {
                //该元素前的一个元素是当前元素20分钟前的元素，应当发起报警
            }
            SensorAndTimeCalculator.CompleteOriginalData(item, last.OriginalData.getDoubleValue("OpeningAperture"), last.OriginalData.getDoubleValue("SensorLUX"), last.OriginalData.getBooleanValue("SensorOCC"), true);
        } else {
            SensorAndTimeCalculator.CompleteOriginalData(item, 0, 0, false, false);
        }

        //判断是否报警
        item.SetIsWarning(SensorAndTimeCalculator.CalculateWarning(deque, item));

        //过时数据退队
        while (deque.size() > 0 && deque.getFirst().TimeStamp + 20 * 60 * 1000 < item.TimeStamp) {
            deque.removeFirst();
        }
        deque.addLast(item);
    }

    public static void SetTime(int start, int end) {
        WorkStartTime = start;
        WorkEndTime = end;
    }

    /**
     * 计算是否应当报警
     *
     * @param deque
     * @param item
     * @return true，报警；false，不报警
     */
    private static boolean CalculateWarning(Deque<WindowedIllumination> deque, WindowedIllumination item) {
        if (SensorAndTimeCalculator.IsWorkTime(item.TimeStamp)) {
            return false;
        }
        //非工作时间
        else {
            boolean openingAperture = item.OriginalData.getDoubleValue("OpeningAperture") > 0;
            //开状态
            if (openingAperture) {
                boolean historySensorOCC = item.OriginalData.getBooleanValue("SensorOCC");
                for (WindowedIllumination dequeItem : deque) {
                    historySensorOCC = historySensorOCC || dequeItem.OriginalData.getBooleanValue("SensorOCC");
                }
                //人感为真
                if (historySensorOCC) {
                    return false;
                }
                //人感为假
                else {
                    return true;
                }
            } else {
                return false;
            }
        }
    }


    /**
     * 补全一个WindowedIllumination中缺失的部分
     *
     * @param item            被补全对象
     * @param openingAperture 被补全对象缺少开度时的开度值
     * @param sensorLUX       被补全对象缺少照度时的照度值
     * @param sensorOCC       被补全对象缺少人感时的人感值
     */
    private static void CompleteOriginalData(WindowedIllumination item, double openingAperture, double sensorLUX, boolean sensorOCC, boolean hasLast) {
        String m = "无历史数据";
        if (hasLast) {
            m = "有前二十分之内的数据";
        }
        if (!item.OriginalData.containsKey("OpeningAperture")) {
            item.SetOpeningApertureValue(openingAperture);
            item.AppendMessage("添加开度," + m);
        }
        if (!item.OriginalData.containsKey("SensorLUX")) {
            item.SetSensorLUXValue(sensorLUX);
            item.AppendMessage("添加照度," + m);
        }
        if (!item.OriginalData.containsKey("SensorOCC")) {
            item.SetSensorOCCValue(sensorOCC);
            item.AppendMessage("添加人感," + m);
        }
    }


    /**
     * @return true是工作时间 false 非工作时间（包括休息日）
     */
    private static boolean IsWorkTime(long timeStamp) {
        Calendar c = Calendar.getInstance(Locale.CHINA);
        c.setTimeInMillis(timeStamp);
        int hour = c.get(Calendar.HOUR_OF_DAY);
        boolean isHoliday = c.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || c.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY;
        if (isHoliday || hour >= WorkEndTime || hour < WorkStartTime) {
            return false;
        } else {
            return true;
        }
    }
}
