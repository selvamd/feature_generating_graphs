package fgg.utils;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLConnection;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MsgGen {

    public static void main(String[] args) throws Exception {
        // Create a dom from the xml.
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(new File(args[0]));
        // Pass the root element as argument to create a
        // ConfigurationProperties object.
        Element rootEle = (Element) doc.getDocumentElement();
        NodeList list = rootEle.getElementsByTagName("struct");
        for (int i =0; i < list.getLength();i++)
        {
            Element ele = (Element)list.item(i);
            String homename = ele.getElementsByTagName("name").item(0).getChildNodes().item(0).getNodeValue();
            Source src = new DOMSource(ele);
            TransformerFactory transFact = TransformerFactory.newInstance();
            Transformer transHome = transFact.newTransformer(new StreamSource(new File(args[1])));
            transHome.transform(src, new StreamResult(new File(args[2]+"//"+homename+".java")));
        }
    }
}
