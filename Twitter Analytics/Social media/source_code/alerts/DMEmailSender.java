/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package alerts;

import config.DMConfigurationConstants;
import config.DMConfigurationLoader;
import engine.DMController;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import loader.DMVerticaConnector;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import writer.DMTargetWritter;

/**
 *
 * @author baradp
 */
public class DMEmailSender extends Thread {

    public static String userName = "hpsmpoc@gmail.com";
    public static String nativeUserName = "pulkit.barad@hp.com";

//    public static String recipientList = "pulkit.barad@hp.com";
//    public static String subjectDP = "Visualization_of_Identified_Patterns";
    public static String password = "hpsmpoc123";
    public static String smtpHost = "smtp.gmail.com";
    public static String smtpPort = "587";
    public static int dpEmailFrequencyPerDay = 12;
    public static long nextDPAlertTimeLong = -1;
    public static boolean javaNativeMethodFailure = false;
    public static JSONObject emailConfigObject;

//    public static String emailBodyDP = "Dear All,\n"
//            + "Please open the attached HTML file in your web browser to Visualize patterns identified from available data sets.\n"
//            + "\n\n--This is System generated email.\nPlease do not reply!";
    @Override
    public void run() {
        long intervalLong = 24 * 60 * 60 * 1000 / 12;

        if (smtpHost != null && !smtpHost.isEmpty()) {
            while (true) {
                Long currTimeLong = System.currentTimeMillis();
                if (nextDPAlertTimeLong == -1 || currTimeLong >= nextDPAlertTimeLong) {
                    try {

                        if (sendDataPatternEmail() == 0) {
                            DMTargetWritter.log("email for Pattern Visualization sent");
                            nextDPAlertTimeLong = currTimeLong + intervalLong;
                            DMTargetWritter.log("Next Pattern Visualization notification time:" + new SimpleDateFormat("dd/MM/yyy-HH:mm:ss").format(new Date(nextDPAlertTimeLong)));

                        }

                    } catch (IOException ex) {
                        javaNativeMethodFailure = true;
                        DMTargetWritter.log("Error encountered while sending email for Pattern Visualization.");
                        DMTargetWritter.log(ex);
                    } catch (MessagingException ex) {
                        javaNativeMethodFailure = true;

                        DMTargetWritter.log("Error encountered while sending email for Pattern Visualization.");
                        DMTargetWritter.log(ex);
                    }
                }
            }
        }
    }

    public static void init(JSONObject emailConfigObject) {
        smtpHost = (String) emailConfigObject.get(DMConfigurationConstants.APP_EMAIL_SMTP_HOST);
        smtpPort = (String) emailConfigObject.get(DMConfigurationConstants.APP_EMAIL_SMTP_PORT);
        userName = (String) emailConfigObject.get(DMConfigurationConstants.APP_EMAIL_SMTP_USER_NAME);
        password = (String) emailConfigObject.get(DMConfigurationConstants.APP_EMAIL_SMTP_PASSWORD);
        nativeUserName = (String) emailConfigObject.get(DMConfigurationConstants.APP_EMAIL_SMTP_NATIVE_USER_NAME);
        DMEmailSender.emailConfigObject = emailConfigObject;

    }

    public static int sendDataPatternEmail() throws IOException, MessagingException {
        if (!DMController.dbPatternReaderLock && DMController.patternToRecordCountMap.size() > 0) {
            DMTargetWritter.log("Sending email for Pattern Visualization ");

            JSONObject patternObject = (JSONObject) emailConfigObject.get(DMConfigurationConstants.APP_EMAIL_PATTERN_NOTIFICATION);
            JSONArray recipientList = (JSONArray) patternObject.get(DMConfigurationConstants.APP_EMAIL_RECIPIENT);
            Iterator<String> recipientIterator = (Iterator<String>) recipientList.iterator();
            String recipientListString = "";
            boolean first = true;
            while (recipientIterator.hasNext()) {
                if (!first) {
                    recipientListString += ",";
                }
                first = false;
                recipientListString += recipientIterator.next();
            }
            String subjectDP = (String) patternObject.get(DMConfigurationConstants.APP_EMAIL_SUBJECT);
            String emailBodyDP = (String) patternObject.get(DMConfigurationConstants.APP_EMAIL_BODY);

            sendEmail(recipientListString, subjectDP, emailBodyDP, DMPatternGenerator.generatePatternFile(DMController.patternToRecordCountMap));
            return 0;
        }
        return -1;
    }

    public static void sendEmail(String recipientList, String subject, String body, String fileName) throws MessagingException, IOException {
        if (!javaNativeMethodFailure && DMConfigurationLoader.proxyHost.isEmpty()) {
            Properties props = new Properties();
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.starttls.enable", "true");
            props.put("mail.smtp.host", smtpHost);
            props.put("mail.smtp.port", smtpPort);

//            props.setProperty("proxySet", "true");
//            props.put("mail.smtp.socks.host", DMConfigurationLoader.proxyPort);
////            props.put("mail.smtp.socks.port", DMConfigurationLoader.proxyHost);
//            props.setProperty("socksProxyHost", DMConfigurationLoader.proxyHost);
//            props.setProperty("socksProxyPort", String.valueOf(DMConfigurationLoader.proxyPort));
////            props.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
////            props.setProperty("mail.smtp.socketFactory.fallback", "false");
////         props.setProperty("mail.smtp.port", "465");
////            props.setProperty("mail.smtp.socketFactory.port", smtpPort);
////            props.put("mail.smtp.auth", "true");
////            props.put("mail.debug", "true");
//
//        }
            Session session = Session.getInstance(props,
                    new javax.mail.Authenticator() {
                        protected PasswordAuthentication getPasswordAuthentication() {
                            return new PasswordAuthentication(userName, password);
                        }
                    });
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(userName));
            message.setRecipients(Message.RecipientType.TO,
                    InternetAddress.parse(recipientList));
            message.setRecipients(Message.RecipientType.CC,
                    InternetAddress.parse(userName));
            message.setSubject(subject);
            BodyPart messageBodyPart = new MimeBodyPart();

            // Create a multipart message for attachment
            Multipart multipart = new MimeMultipart();

            // Set text message part
            messageBodyPart.setText(body);
            multipart.addBodyPart(messageBodyPart);

            // Second part is attachment
            messageBodyPart = new MimeBodyPart();
            if (new File(fileName).exists()) {

                DataSource source = new FileDataSource(fileName);

                messageBodyPart.setDataHandler(new DataHandler(source));
                messageBodyPart.setFileName(new File(fileName).getName());
                multipart.addBodyPart(messageBodyPart);
            }

            // Send the complete message parts
            message.setContent(multipart);
            Transport.send(message);
        } else {
            if (DMConfigurationLoader.directorySeparator.equals("/")) {
                String command = "";

                command = "mailx -a " + new File(fileName).getCanonicalPath() + " -r " + nativeUserName + " -s \"" + subject + "\" " + recipientList;
                try {
                    DMTargetWritter.log("Sending Data Pattern email from command line:" + command);
                    OutputStream outStream = Runtime.getRuntime().exec(command).getOutputStream();
                    outStream.write(body.getBytes());
                    outStream.flush();
                    outStream.close();
                } catch (IOException ex) {
                    DMTargetWritter.log(ex);
                }
            }
        }
    }
}
