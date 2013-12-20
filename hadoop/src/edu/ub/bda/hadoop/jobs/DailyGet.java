package edu.ub.bda.hadoop.jobs;

import java.io.*;
import java.net.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import javax.swing.text.MutableAttributeSet;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;

/**
 * This is a task implementation to download wikidumps.
 *
 * @author domenicocitera
 */
public class DailyGet
{
    
    /**
     * Flag used to enable download report
     */
    private static boolean reportDownload = true;
    private static boolean dev = false;
    
    private static String tmpPath = "./";
    
    /**
     * Query used to import wikidumps to the hive table
     */
    private static String formatQuery = "LOAD DATA LOCAL INPATH ''{0}'' OVERWRITE INTO TABLE dcitera_olopez.bda_wikidump_dcitera_olopez PARTITION (ds=''{1}'')";
    
    /**
     * In order to download arbitrary hours for a day, put in this string any hour
     * not desired and separate it by commas
     */
    private static String filter = "";
    
    /**
     * Pattern used to search valid links to download
     */
    private static Pattern linkPattern = Pattern.compile("pagecounts-(\\d{4})(\\d{2})(\\d{2})-(\\d{2})(\\d{2})\\d{2}.gz");

    public static void main(String[] args)
    {
        try
        {
            String year = null, month = null, day = null, hour = null, minute = null;
            
            if ( dev )
            {
                year = "2013";
                month = "11";
                day = "05";
                hour = "01";
                minute = "00";
                tmpPath = "./tmp/";
            }
            else 
            {
                if ( args.length == 3 || args.length == 5)
                {
                    if ( args.length == 3 )
                    {
                        year = args[0];
                        month = args[1];
                        day = args[2];
                    }
                    
                    if ( args.length == 5 )
                    {
                        year = args[0];
                        month = args[1];
                        day = args[2];
                        hour = args[3];
                        minute = args[4];
                    }
                }
                else
                {
                    throw new Exception("Bad arguments");
                }
            }
           
            Matcher matcher = null;
            
            String baseUrl = "http://dumps.wikimedia.org/other/pagecounts-raw/" + year + "/" + year + "-" + month + "/";
           
            URL url = new URL(baseUrl + "index.html");
            String userAgent = "Mozilla/5.0 (X11; U; Linux i686; it; rv:1.9.0.10) Gecko/2009042513 Ubuntu/8.04 (hardy) Firefox/3.0.10";

            System.out.println("Downloading  index file...");
            downloadFileFromUrl(url, tmpPath + "index.html", userAgent);
            System.out.println("Done downloading index");
            
            FileReader reader = new FileReader(tmpPath + "index.html");
            List<String> links = LinkExtractor(reader);
            (new File(tmpPath + "index.html")).delete();
            
            List<String> filterlinks = new ArrayList();
            for (String link : links)
            {
                if ( hour != null && minute != null )
                {
                    if ( link.matches("pagecounts-" + year + month + day + "-" + hour + minute +  "\\d{2}.gz") ){
                        filterlinks.add(link);
                    }
                }
                else
                {
                    if ( link.matches("pagecounts-" + year + month + day + "-\\d{6}.gz") ){
                        String lnkHour = link.substring(20, 22);
                        if ( !filter.contains(lnkHour) ){
                            filterlinks.add(link);
                        }
                    }
                }
            }
            
            if ( filterlinks.isEmpty() )
            {
                System.out.println("Warning! No URL matches requirements. Shutting down.");
            }

            int i = 1;
            for (String link : filterlinks) {
                String gzipPath = tmpPath + link;
                String unzPath = tmpPath + link.replaceAll(".gz", "");
                
                System.out.println("Downloading " + link + "... [" + i + "/" + filterlinks.size() + "]");
                
                url = new URL(baseUrl + link);
                downloadFileFromUrl(url, gzipPath, userAgent);
                gzUnzipper(gzipPath, unzPath);
                (new File(gzipPath)).delete();
                
                matcher = linkPattern.matcher(link);
        
                while ( matcher.find() )
                {
                    year = matcher.group(1);
                    month = matcher.group(2);
                    day = matcher.group(3);
                    hour = matcher.group(4);
                    minute = matcher.group(5);
                }
            
                File f = new File(unzPath);
                String query = MessageFormat.format(formatQuery, f.getAbsolutePath(), year + month + day + "-" + hour + minute);
                executeHiveQueryCli(query);
                f.delete();
                
                i++;
            }
            
        } catch ( Exception e ) {
            e.printStackTrace();
        }
        
        System.out.println("Job ended.");
    }

    /**
     * This method downloads a file to the local path, with a User Agent specified.
     *
     * @param url The file to download
     * @param localFile The path where to save the file
     * @param userAgent The User Agent to use to fake the request sender.
     * @throws IOException
     */
    public static void downloadFileFromUrl(URL url, String localFile, String userAgent) throws IOException
    {
        InputStream is = null;
        FileOutputStream fstream = null;

        try
        {
            URLConnection urlConn = url.openConnection();

            if (userAgent != null)
            {
                urlConn.setRequestProperty("User-Agent", userAgent);
            }
            urlConn.connect();
            
            long fileSize = urlConn.getContentLength();
            if ( fileSize != -1 )
            {
                System.out.println("Filesize: " + fileSize);
            }

            is = urlConn.getInputStream();
            fstream = new FileOutputStream(localFile);

            byte[] buffer = new byte[4096];
            int len;
            long bytes = 0;
            long p = 0;
            long op = -1;
            while ((len = is.read(buffer)) > 0)
            {
                fstream.write(buffer, 0, len);
                
                if ( reportDownload && fileSize != -1 )
                {
                    bytes += len;
                    p = bytes * 100 / fileSize;
                    
                    if ( op != p )
                    {
                        System.out.println("Downloaded " + p + "%");
                        op = p;
                    }
                }
            }
        }
        finally
        {
            try
            {
                if (is != null)
                {
                    is.close();
                }
            }
            finally
            {
                if (fstream != null)
                {
                    fstream.close();
                }
            }
        }
    }

    /**
     * Return a list of links given a file reader pointing to an HTML page.
     * 
     * @param reader The file reader
     * @return A list of Strings containing the HREF attribute content
     * @throws IOException 
     */
    public static List<String> LinkExtractor(Reader reader) throws IOException
    {
        final ArrayList<String> list = new ArrayList<String>();
        ParserDelegator pd = new ParserDelegator();
        HTMLEditorKit.ParserCallback pc;
        pc = new HTMLEditorKit.ParserCallback() {
            
            @Override
            public void handleText(final char[] date, final int pos) {
            }

            @Override
            public void handleStartTag(HTML.Tag tag, MutableAttributeSet attr, int pos) {
                if (tag == HTML.Tag.A) {
                    String address = (String) attr.getAttribute(HTML.Attribute.HREF);
                    list.add(address);
                }
            }

            @Override
            public void handleEndTag(HTML.Tag t, final int pos) {
            }

            @Override
            public void handleSimpleTag(HTML.Tag t, MutableAttributeSet a, final int pos) {
            }

            @Override
            public void handleComment(final char[] data, final int pos) {
            }

            @Override
            public void handleError(final java.lang.String errMsg, final int pos) {
            }
        };
        pd.parse(reader, pc, false);
        return list;
    }

    
    /**
     * A method to gUnZip a file and output it to a specific path.
     * 
     * @param gZip_file Path to the GZIP file
     * @param out_file Path where to output the GZIP contents
     */
    public static void gzUnzipper(String gZip_file, String out_file)
    {
        byte[] buffer = new byte[1024];

        try {
            GZIPInputStream gzs = new GZIPInputStream(new FileInputStream(gZip_file));
            FileOutputStream out = new FileOutputStream(out_file);

            int len;
            while ((len = gzs.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }

            gzs.close();
            out.close();

            System.out.println("Done unzipping file");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    
    /**
     * Execute a process to execute a Hive Query.
     * 
     * @param query The query to be executed by Hive
     */
    public static void executeHiveQueryCli(String query) throws Exception
    {
        System.out.println("Executing query: " + query);
        
        ProcessBuilder builder = new ProcessBuilder("hive", "-e", query);
        Process process = builder.start();
        process.waitFor();
        
        int ret = process.exitValue();
        
        System.out.println("Log:");
        //printStream(process.getInputStream());
        
        if ( ret != 0 )
        {
            System.out.println("Something went wrong when importing the data to Hive.");
            //printStream(process.getErrorStream());
            
            throw new Exception("Shell command execution went awry");
        }
    }
    
    /**
     * Helper method to allow to print streams.
     * 
     * @param stream The stream
     */
    private static void printStream(InputStream stream) throws Exception
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line = reader.readLine();
        while ( line != null )
        {
            System.out.println("  " + line);
        }
    }
}
