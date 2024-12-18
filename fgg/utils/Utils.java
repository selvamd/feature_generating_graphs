package fgg.utils;

import fgg.data.*;
import java.util.*;
import java.io.*;
import fgg.access.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.util.Base64;

public class Utils
{
	public static long makelong(int msb, int lsb) {
		return (((long)(msb)) << 32) | (lsb & 0xffffffffL); }

	public static int msb(long l) { return (int)(l>>32); }
	public static int lsb(long l) { return (int)l; 		 }
	public static int mindate() { return 20121231; 		 }

	public static int today() {
        try {
            return Integer.parseInt(Persistor.today().replaceAll("-",""));
        } catch (Exception e) {}
        return -1;
    }

    //Splits the csv with quoted string containing commas
    public static String[] parseCsv(String aline) {
        String[] arr = aline.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        for (int i=0;i<arr.length;i++)
            if (arr[i].charAt(0)=='"')
                arr[i] = arr[i].substring(1,arr[i].length()-1);
        return arr;
    }

    public List<String> tokenize(String str, String sub, List<String> out)
    {
        out.clear();
        int idx = str.indexOf(sub);
        while (idx > 0)
        {
            out.add(str.substring(0, idx));
            str = str.substring(sub.length()+idx);
            idx = str.indexOf(sub);
        }
        out.add(str);
        return out;
    }

    //If EOM_ONLY=TRUE, it returns every end of month and yesterday dates
	public static List<Integer> dates(int from, boolean EOM_ONLY)
    {
        List<Integer> dates = new ArrayList<Integer>();
		Calendar cal = Calendar.getInstance();
        int today = cal.get(Calendar.YEAR) * 10000 +
					(1+cal.get(Calendar.MONTH)) * 100 +
					cal.get(Calendar.DAY_OF_MONTH);

        int dt = from, ndt = 0;
        while (dt < today) {
            if (!EOM_ONLY) dates.add(dt);
            ndt = addDate(dt,1);
            if (EOM_ONLY && ndt % 100 == 1)
                dates.add(dt);
            if (ndt == today)
                dates.add(dt);
            dt = ndt;
        }
        return dates;
    }

	public static int addDate(int dt, int days)
    {
		Calendar cal = Calendar.getInstance();
		cal.set(dt/10000, ((dt%10000)/100) - 1, dt%100);
		cal.add(Calendar.DATE, days);
		return cal.get(Calendar.YEAR) * 10000 +
					(1+cal.get(Calendar.MONTH)) * 100 +
					cal.get(Calendar.DAY_OF_MONTH);
	}

    //Returns db compatible date in string format
    public static String dbDate(int dt) {
        String sdt = ""+dt;
        if (sdt.length() != 8) return null;
        if (dt < 19000000 || dt > 21000000)
            if (dt != 99991231)
                return null;
        return sdt.substring(0,4) + "-" +
                sdt.substring(4,6) + "-" +
                sdt.substring(6);
    }

	public static String loadsql(String file) throws Exception
	{
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String aline = null, result = "";
		while ((aline = reader.readLine()) != null)
		{
			aline = aline.trim();
			if (aline.startsWith("--"))
				continue;
			if (aline.indexOf(" --") > 0)
				aline = aline.substring(0, aline.indexOf(" --"));
			result += aline + " ";
		}
		return result;
	}

	public static String loadSqlTemplate(String file, String[] args) throws Exception
	{
		String sql = loadsql(file);
		if (args == null) return sql;
		for (int i=0;i<args.length;i++)
		{
			int index = -1;
			while ((index = sql.indexOf("$ARG"+i)) > 0)
			{
				String str = sql.substring(0,index) + args[i];
				sql = str + sql.substring(index+("$ARG"+i).length());
			}
		}
		return sql;
	}

	private static String auth = "6PzU1NJ4uofWeZBY5uAiwWVIOGN9NHOK";
    private static final char[] PASSWORD = "enfldsgbnlsngdlksdsgm".toCharArray();
    private static final byte[] SALT = { (byte) 0xde, (byte) 0x33, (byte) 0x10, (byte) 0x12, (byte) 0xde, (byte) 0x33, (byte) 0x10, (byte) 0x12, };
    public static String decrypt(String property) {
		try {
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
			SecretKey key = keyFactory.generateSecret(new PBEKeySpec(PASSWORD));
			Cipher pbeCipher = Cipher.getInstance("PBEWithMD5AndDES");
			pbeCipher.init(Cipher.DECRYPT_MODE, key, new PBEParameterSpec(SALT, 20));
			return new String(pbeCipher.doFinal(Base64.getDecoder().decode(property)), "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
    }

	public static String prompt(String str) {
		try {
	        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			String aline = null;
			while (aline == null)
			{
				System.out.print(str + "\t");
				aline=reader.readLine();
				if (aline.trim().length() > 0) {
					System.out.println("Output:");
					return aline;
				}

				aline = null;
			}
		} catch (Exception e) {
		}
		return null;
	}

    public static String encrypt(String property)
	{
		try {
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
			SecretKey key = keyFactory.generateSecret(new PBEKeySpec(PASSWORD));
			Cipher pbeCipher = Cipher.getInstance("PBEWithMD5AndDES");
			pbeCipher.init(Cipher.ENCRYPT_MODE, key, new PBEParameterSpec(SALT, 20));
			String pwd = Base64.getEncoder().encodeToString(pbeCipher.doFinal(property.getBytes("UTF-8")));
			System.out.println(pwd);
			return pwd;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
    }

}
