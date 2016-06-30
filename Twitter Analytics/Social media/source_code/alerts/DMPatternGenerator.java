/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package alerts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import writer.DMTargetWritter;

/**
 *
 * @author baradp
 */
public class DMPatternGenerator {

//    public static int frameWidth = 2400;
//    public static int frameHeight = 1350;
    public static int minFluctuationPercent = 100;
    public static int minFluctuationRecord = 10;
    public static String patternFileName = "generated/PatternWordCloud.htm";
    public static String patternFileContent;
    public static int maxFontSize = 50;
    public static int minFontSize = 10;

    public static String generatePatternFile(HashMap<String, Integer> patternToRecordMap) throws IOException {

        Random random = new Random();
        boolean first = true;
        int totalCount = 1;
        int yPositive = 50;
        int yNegative = ((patternToRecordMap.size() / 4) + 1) * 60 + 200;
        int xPositive = ((patternToRecordMap.size() / 4) + 1) * 60 + 200;
        int xNegative = 50;
        int xUnitPosMovement = 25;
        int yUnitPosMovement = 25;
        int r = 0, g = 0, b = 0;
        int frameHeight = yNegative;
        int frameWidth = xPositive;
        patternFileContent = "<html>\n<body>\n<svg height=\"" + frameHeight + "px\"" + "width=\"" + (frameWidth + 400) + "px\">\n";

        int maxValue = 0;
        String maxPattern = "";
        int inputTotal = 0;
        for (Map.Entry<String, Integer> entry : patternToRecordMap.entrySet()) {
            if (entry.getValue() > maxValue) {
                maxValue = entry.getValue();
                maxPattern = entry.getKey();
            }
            inputTotal += entry.getValue();
        }
        int inputAvg = inputTotal / patternToRecordMap.size();
        for (Map.Entry<String, Integer> entry : patternToRecordMap.entrySet()) {
            int size = 20;
            int weightage = (entry.getValue() * 30) / inputAvg;
            if (weightage > 50) {
                size = 50;
            } else if (weightage < 10) {
                size = 10;
            }
            if (totalCount % 3 == 1) {
                r = 0;
                g = 0;
                b = random.nextInt(200);
            } else if (totalCount % 3 == 2) {
                r = 0;
                b = 0;
                g = random.nextInt(200);
            } else if (totalCount % 3 == 0) {
                b = 0;
                g = 0;
                r = random.nextInt(200);
            }
            int x = 0, y = 0, angle = 0;
            if (entry.getKey().equals(maxPattern)) {
                x = frameWidth / 2;
                y = frameHeight / 2 + 100;
                r = 180;
                g = 0;
                b = 0;
            } else {
                if (totalCount % 4 == 1) {
                    x = frameWidth / 2;
                    y = yNegative;
                    if (size > 30) {
                        yNegative -= yUnitPosMovement;
                        r = 0;
                        b = 0;
                        g = 0;
                    }
                    yNegative -= yUnitPosMovement;
                } else if (totalCount % 4 == 3) {
                    x = frameWidth / 3;
                    y = yPositive;
                    if (size > 30) {
                        yPositive += yUnitPosMovement;
                        r = 0;
                        b = 0;
                        g = 0;
                    }
                    yPositive += yUnitPosMovement;

                } else if (totalCount % 4 == 2) {
                    x = xNegative;
                    if (size > 30) {
                        xNegative += xUnitPosMovement;
                        r = 0;
                        b = 0;
                        g = 0;
                    }
                    xNegative += xUnitPosMovement;

                    y = frameHeight / 2;
                    angle = 90;

                } else if (totalCount % 4 == 0) {
                    x = xPositive + 400;
                    if (size > 30) {
                        xPositive -= xUnitPosMovement;
                        r = 0;
                        b = 0;
                        g = 0;
                    }
                    xPositive -= xUnitPosMovement;

                    y = (frameHeight / 2)+200;
                    angle = 270;

                }
                totalCount++;

            }
            patternFileContent += "\t<text fill=\"rgb(" + r + "," + g + "," + b + ")\" "
                    + "font-family=\"Verdana\" " + " font-size=\"" + size + "\" transform=\"translate(" + x + "," + y + ")rotate(" + angle + ")\">"
                    + entry.getKey() + "</text>\n";
        }
        patternFileContent += "</svg>\n</body>\n<html>";
        File file = new File(patternFileName).getCanonicalFile();
        file.getParentFile().mkdirs();
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(patternFileContent);
        patternFileContent="";
        DMTargetWritter.log("Pattern File generated at :"+file.getCanonicalPath());
        writer.close();

        return file.getCanonicalPath();
    }
}
