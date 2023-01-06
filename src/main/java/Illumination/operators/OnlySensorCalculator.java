package Illumination.operators;

import Illumination.models.WindowedIllumination;

import java.util.Deque;

public class OnlySensorCalculator {

    /**
     * 计算是否应当报警，并补全缺失的数据
     */
    public static void Calculate(Deque<WindowedIllumination> deque, WindowedIllumination item) {
        //补全缺失的数据
        if (deque.size() == 0) {
            OnlySensorCalculator.CompleteOriginalData(item, 0, 0, false, false);
        } else {
            WindowedIllumination last = deque.peekLast();
            OnlySensorCalculator.CompleteOriginalData(item, last.OriginalData.getDoubleValue("OpeningAperture"), last.OriginalData.getDoubleValue("SensorLUX"), last.OriginalData.getBooleanValue("SensorOCC"), true);
        }

        //判断是否报警
        item.SetIsWarning(OnlySensorCalculator.CalculateWarning(deque, item));

        //过时数据退队
        while (deque.size() > 0 && deque.getFirst().TimeStamp + 20 * 60 * 1000 < item.TimeStamp) {
            deque.removeFirst();
        }
        deque.addLast(item);
    }

    /**
     * 计算是否应当报警
     *
     * @param deque
     * @param item
     * @return true，报警；false，不报警
     */
    private static boolean CalculateWarning(Deque<WindowedIllumination> deque, WindowedIllumination item) {
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
        } else {

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
}
