using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Drawing;
using System.Drawing.Imaging;

namespace Helpers
{
    /// <summary>
    /// 不常用工具箱
    /// </summary>
    public static class Helper
    {
        /// <summary>
        /// 文件输出至指定路径
        /// </summary>
        /// <param name="path">文件物理路径，例：C:\test.txt</param>
        /// <param name="content">要输出的内容，以utf-8的格式</param>
        /// <returns>输出结果</returns>
        public static bool PrintTXTFile(string path, string content)
        {
            try
            {
                StreamWriter sw = new StreamWriter(path, false, Encoding.UTF8);
                sw.WriteLine(content);
                sw.Close();
                sw.Dispose();
                return true;
            }
            catch (Exception ex)
            {
                Tool.PrintLog("添加内容失败", ex.Message);
                return false;
            }
        }

        /// <summary>
        /// 取得列表类别
        /// </summary>
        /// <param name="number">type</param>
        /// <returns></returns>
        public static string GetType(int number)
        {
            string str = "";
            switch (number)
            {
                case -1: str = "最新添加"; break;
                case 0: str = "学习路上"; break;
                case 1: str = "个人私文"; break;
                case 2: str = "加密"; break;
                case 3: str = "随笔记"; break;
                default: str = ""; break;
            }
            return str;
        }

        /// <summary>
        /// 无效字符清空
        /// </summary>
        /// <param name="str">待过滤的字符串</param>
        /// <param name="list">要过滤的字符列表</param>
        /// <returns>过滤掉无效内容后的字符串</returns>
        public static string FilterStr(string str, List<string> list)
        {
            if (string.IsNullOrWhiteSpace(str)) { return ""; }
            foreach (var a in list)
            {
                str = Helpers.Tool.ReplaceString(str, a, "");
            }
            return str;
        }

        /// <summary>
        /// 处理html常见标签的字符串
        /// </summary>
        /// <param name="str">待处理的字符串</param>
        /// <returns>处理后的字符串</returns>
        public static string HtmlCodeStr(string str)
        {
            str = Helpers.Tool.ReplaceString(str, "<html>", "");
            str = Helpers.Tool.ReplaceString(str, "</html>", "\f");
            str = Helpers.Tool.ReplaceString(str, "<body>", "");
            str = Helpers.Tool.ReplaceString(str, "</body>", "\n");
            str = Helpers.Tool.ReplaceString(str, "<head>", "");
            str = Helpers.Tool.ReplaceString(str, "</head>", "\n");
            str = Helpers.Tool.ReplaceString(str, "<div>", "");
            str = Helpers.Tool.ReplaceString(str, "</div>", "\n");
            str = Helpers.Tool.ReplaceString(str, "<p>", "\r");
            str = Helpers.Tool.ReplaceString(str, "</p>", "\r");
            str = Helpers.Tool.ReplaceString(str, "<hr>", "\r");
            str = Helpers.Tool.ReplaceString(str, "<a>", "");
            str = Helpers.Tool.ReplaceString(str, "</a>", "");
            str = Helpers.Tool.ReplaceString(str, "<content>", "\n");
            str = Helpers.Tool.ReplaceString(str, "</content>", "\n");
            str = Helpers.Tool.ReplaceString(str, "<iframe>", "\n");
            str = Helpers.Tool.ReplaceString(str, "</iframe>", "\n");
            str = Helpers.Tool.ReplaceString(str, "<h1>", "");
            str = Helpers.Tool.ReplaceString(str, "</h1>", "");
            str = Helpers.Tool.ReplaceString(str, "<h2>", "");
            str = Helpers.Tool.ReplaceString(str, "</h2>", "");
            str = Helpers.Tool.ReplaceString(str, "<h3>", "");
            str = Helpers.Tool.ReplaceString(str, "</h3>", "");
            str = Helpers.Tool.ReplaceString(str, "<h4>", "");
            str = Helpers.Tool.ReplaceString(str, "</h4>", "");
            str = Helpers.Tool.ReplaceString(str, "<i>", "");
            str = Helpers.Tool.ReplaceString(str, "</i>", "");
            str = Helpers.Tool.ReplaceString(str, "&gt;", "<");
            str = Helpers.Tool.ReplaceString(str, "&lt;", ">");
            str = Helpers.Tool.ReplaceString(str, "&nbsp;", " ");
            str = Helpers.Tool.ReplaceString(str, "<br>", "\n\r");
            str = Helpers.Tool.ReplaceString(str, "<br />", "\n\r");
            str = Helpers.Tool.ReplaceString(str, "<Br>", "\n\r");
            str = Helpers.Tool.ReplaceString(str, "<Br />", "\n\r");
            str = Helpers.Tool.ReplaceString(str, "\"", "");
            return str;
        }

        /// <summary>
        /// IPV4地址验证
        /// </summary>
        /// <returns></returns>
        public static bool IpAddressSign(string ip)
        {
            try
            {
                if (ip == "::1") { return true; }
                ip = ip.Trim();
                string[] ips = ip.Split('.');
                if (ips.Length != 4) { return false; }
                foreach (var a in ips)
                {
                    int splitAddr = int.Parse(a);
                    if (splitAddr < 0 && splitAddr > 256) { return false; }
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 32位大写MD5加密
        /// </summary>
        /// <param name="str">待加密的字符串</param>
        /// <returns></returns>
        public static string MD5(string str)
        {
            string result = MD5(str, true, 32);
            return result;
        }

        /// <summary>
        /// 32位MD5加密
        /// </summary>
        /// <param name="str">待加密的字符串</param>
        /// <param name="IsUpper">是否大写</param>
        /// <returns></returns>
        public static string MD5(string str, bool IsUpper)
        {
            string result = MD5(str, IsUpper, 32);
            return result;
        }

        /// <summary>
        /// MD5加密
        /// </summary>
        /// <param name="str">待加密的字符串</param>
        /// <param name="IsUpper">是否大写</param>
        /// <param name="bit">加密位数16或者32</param>
        /// <returns></returns>
        public static string MD5(string str, bool IsUpper, int bit)
        {
            System.Security.Cryptography.MD5 md = new System.Security.Cryptography.MD5CryptoServiceProvider();
            byte[] byteStream = System.Text.Encoding.UTF8.GetBytes(str);
            byte[] temp = md.ComputeHash(byteStream);
            StringBuilder tempStr = new StringBuilder();
            string result = null;
            foreach (var a in temp)
            {
                tempStr.Append(a.ToString("x2"));
            }
            if (IsUpper)
            {
                result = tempStr.ToString().ToUpper();
            }
            else
            {
                result = tempStr.ToString().ToLower();
            }
            if (bit == 16)
            {
                result = result.Substring(8, 16);
            }
            else if (bit == 32)
            {
                result = result.ToString();
            }
            else
            {
                result = string.Empty;
            }
            return result;
        }

        /// <summary>
        /// 验证身份证号格式
        /// </summary>
        /// <param name="code">身份证号</param>
        /// <returns>正确或错误</returns>
        public static bool ValidateIdentityCode(string code)
        {
            if (code.Length != 15 && code.Length != 18) { return false; }
            if (code[0] == 48)
            { return false; }
            foreach (char c in code)
            {
                if (c > 47 && c < 58)
                { continue; }
                else if (c == 88 || c == 120)
                {
                    if (code[17] == c)
                    { continue; }
                    else
                    { return false; }
                }
                else
                { return false; }
            }
            return true;
        }

        /// <summary>
        /// 验证手机号格式
        /// </summary>
        /// <param name="mobile">手机号</param>
        /// <returns>正确或错误</returns>
        public static bool ValidateMobile(string mobile)
        {
            if (mobile.Length != 11) { return false; }
            if (mobile[0] != 49) { return false; }
            foreach (char c in mobile)
            {
                if (c < 48 || c > 57)
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// 验证邮箱格式
        /// </summary>
        /// <param name="mail">邮箱账号</param>
        /// <returns>正确或错误</returns>
        public static bool ValidateMail(string mail)
        {

            if (mail.Length < 5) { return false; }

            //反转邮箱账号字符串
            string descMail = null;
            foreach (char c in mail.Reverse())
            {
                descMail += c;
            }

            int atindex = descMail.IndexOf('@');
            int dotindex = descMail.IndexOf('.');
            if (atindex < 3)
            {
                return false;
            }
            if (dotindex < 1)
            {
                return false;
            }

            if (mail[0] == '@' || mail[0] == '.') { return false; }
            if (dotindex > atindex) { return false; }
            return true;
        }

        /// <summary>
        /// 过滤非法Sql字符
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static string SQLFilter(string str)
        {
            if (string.IsNullOrEmpty(str))
            {
                return "";
            }
            List<string> strs = new List<string>();
            strs.Add("'--");
            strs.Add(";");
            strs.Add("|");
            return FilterStr(str, strs);
        }

        /// <summary>
        /// 图片转base64
        /// </summary>
        /// <param name="bmp"></param>
        /// <returns></returns>
        public static string ImgToBase64String(Bitmap bmp)
        {
            try
            {
                MemoryStream ms = new MemoryStream();
                bmp.Save(ms, System.Drawing.Imaging.ImageFormat.Jpeg);
                byte[] arr = new byte[ms.Length];
                ms.Position = 0;
                ms.Read(arr, 0, (int)ms.Length);
                ms.Close();
                return Convert.ToBase64String(arr);
            }
            catch 
            {
                return null;
            }
        }

        /// <summary>
        /// base64保存成图片，返回Bitmap对象
        /// </summary>
        /// <param name="strbase64"></param>
        public static Bitmap Base64StringToImage(string filePath, string fileName, string strbase64, ImageType type)
        {

            try
            {
                byte[] arr = Convert.FromBase64String(strbase64);
                MemoryStream ms = new MemoryStream(arr);
                Bitmap bmp = new Bitmap(ms);

                switch (type)
                {
                    case ImageType.Jpg: bmp.Save($@"{filePath}\{fileName}.jpg", System.Drawing.Imaging.ImageFormat.Jpeg); break;
                    case ImageType.Bmp: bmp.Save($@"{filePath}\{fileName}.bmp", ImageFormat.Bmp); break;
                    case ImageType.Gif: bmp.Save($@"{filePath}\{fileName}.gif", ImageFormat.Gif); break;
                    case ImageType.Png: bmp.Save($@"{filePath}\{fileName}.png", ImageFormat.Png); break;
                    default: bmp.Save($@"{filePath}\{fileName}.jpg", System.Drawing.Imaging.ImageFormat.Jpeg); break;
                }
                //bmp.Save($@"{savePath}\test.jpg", System.Drawing.Imaging.ImageFormat.Jpeg);
                //bmp.Save(@"d:\"test.bmp", ImageFormat.Bmp);
                //bmp.Save(@"d:\"test.gif", ImageFormat.Gif);
                //bmp.Save(@"d:\"test.png", ImageFormat.Png);
                ms.Close();
                return bmp;
            }
            catch (Exception ex)
            {
                Helpers.Tool.PrintLog_Error(ex.Message, ex.StackTrace);
                return null;
            }
        }

        /// <summary>
        /// 创建文件夹
        /// </summary>
        /// <param name="filePath">路径</param>
        /// <param name="folderName">文件夹名</param>
        /// <returns></returns>
        public static string CreateDirectory(string filePath, string folderName)
        {

            if (string.IsNullOrEmpty(filePath))
            {
                if (string.IsNullOrEmpty(folderName))
                {
                    return "";
                }
            }
            else
            {
                if (filePath.Substring(filePath.Length - 1) == @"\")
                {
                    filePath = filePath.Substring(0, filePath.Length - 1);
                }
            }
            string path = filePath + @"\" + folderName;
            try
            {
                if (!System.IO.Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }
            }
            catch (Exception ex)
            {
                Helpers.Tool.PrintLog_Info("出错了！", ex.Message + ex.StackTrace);
                return null;
            }
            Helpers.Tool.PrintLog_Info(path, "");
            return path;
        }

        /// <summary>
        /// 将时间转换成秒级的时间戳
        /// </summary>
        /// <param name="time">要转换的时间</param>
        /// <returns></returns>
        public static int DateTimeToDateStr(DateTime time)
        {
            System.DateTime startTime = TimeZone.CurrentTimeZone.ToLocalTime(new System.DateTime(1970, 1, 1));
            int timeStr = (int)(DateTime.Now - startTime).TotalSeconds;
            return timeStr;
        }
        /// <summary>
        /// 将时间戳转换成C#的时间
        /// </summary>
        /// <param name="dateStr"></param>
        /// <returns></returns>
        public static DateTime DateStrToDateTime(int dateStr)
        {
            DateTime startTime = TimeZone.CurrentTimeZone.ToLocalTime(new DateTime(1970, 1, 1));
            long lTime = long.Parse(dateStr.ToString() + "0000000");
            TimeSpan toNow = new TimeSpan(lTime);
            return Tool.FormatDate(startTime.Add(toNow));
        }

        /// <summary>
        /// 按字典的key升序拼接字典,字典key和value的类型为string
        /// </summary>
        /// <param name="dic"></param>
        /// <returns></returns>
        public static string SplicingDic(Dictionary<string, string> dic)
        {
            if (dic == null)
            {
                return null;
            }
            var temp = dic.OrderBy(o => o.Key, StringComparer.OrdinalIgnoreCase);
            StringBuilder result = new StringBuilder();
            foreach (var a in temp)
            {
                result.Append($"{a.Key}={a.Value}&");
            }
            string str = result.ToString();
            str = str.Substring(0, str.Length - 1);
            return str;
        }

        /// <summary>
        /// ASE加密
        /// </summary>
        /// <param name="text">待加密的文本</param>
        /// <param name="key">密钥（可选）</param>
        /// <param name="iv">位移量（可选）</param>
        /// <returns>加密后的密文</returns>
        public static string ASEEncode(string text,string key= "abcd", string iv= "dddd") {
            Helpers.ASEEncryption ase = new Helpers.ASEEncryption();
            return ase.Encrypt(text,key,iv);
        }

        /// <summary>
        /// ASE解密
        /// </summary>
        /// <param name="text">密文</param>
        /// <param name="key">密钥（可选）</param>
        /// <param name="iv">位移量（可选）</param>
        /// <returns>解密后的文本</returns>
        public static string ASEDecode(string pwdText, string key = "abcd", string iv = "dddd")
        {
            Helpers.ASEEncryption ase = new Helpers.ASEEncryption();
            return ase.Decrypt(pwdText, key, iv);
        }
    }

    /// <summary>
    /// 枚举图片格式
    /// </summary>
    public enum ImageType {
        Jpg = 0,
        Bmp = 1,
        Gif = 2,
        Png = 3
    }
}
