using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;

namespace Helpers
{
    /// <summary>
    /// 个人常用工具箱
    /// </summary>
    public static class Tool
    {
        /// <summary>
        /// 打印日志
        /// </summary>
        /// <returns></returns>
        public static bool PrintLog(string title, string content)
        {
            try
            {
                string path = GetAppSetting("logPath");
                CreateLogDirectory(path);
                //往指定文件中追加日志
                StreamWriter sw = new StreamWriter(string.Format("{0}", path) + DateTime.Now.ToString("yyyy-MM-dd") + "-log.txt", true, Encoding.UTF8);
                sw.WriteLine("产生时间：" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                sw.WriteLine("\t" + title);
                sw.WriteLine(content);
                sw.WriteLine();
                sw.WriteLine();
                sw.Close();
                sw.Dispose();
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 打印错误日志
        /// </summary>
        /// <returns></returns>
        public static bool PrintLog_Error(string title, string content)
        {
            try
            {
                string path = GetAppSetting("error");
                CreateLogDirectory(path);
                //往指定文件中追加日志
                StreamWriter sw = new StreamWriter(string.Format("{0}", path) + DateTime.Now.ToString("yyyy-MM-dd") + "-error_log.txt", true, Encoding.UTF8);
                sw.WriteLine("产生时间：" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                sw.WriteLine("\t" + title);
                sw.WriteLine(content);
                sw.WriteLine();
                sw.WriteLine();
                sw.Close();
                sw.Dispose();
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 打印信息日志
        /// </summary>
        /// <returns></returns>
        public static bool PrintLog_Info(string title, string content)
        {
            try
            {
                string path = GetAppSetting("info");
                CreateLogDirectory(path);
                //往指定文件中追加日志
                StreamWriter sw = new StreamWriter(string.Format("{0}", path) + DateTime.Now.ToString("yyyy-MM-dd") + "-info_log.txt", true, Encoding.UTF8);
                sw.WriteLine("产生时间：" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                sw.WriteLine("\t" + title);
                sw.WriteLine(content);
                sw.WriteLine();
                sw.WriteLine();
                sw.Close();
                sw.Dispose();
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 打印字符串到txt
        /// </summary>
        /// <param name="content">打印的内容</param>
        /// <param name="path">文件路径(不含后缀)</param>
        /// <param name="isAppend">是否为追加</param>
        /// <returns></returns>
        public static bool PrintStrToTxt(string content, string path, bool isAppend)
        {
            try
            {
                //往指定文件中追加日志
                StreamWriter sw = new StreamWriter(path + ".txt", isAppend, Encoding.UTF8);
                sw.WriteLine(content);
                sw.WriteLine();
                sw.Close();
                sw.Dispose();
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 读取指定的文件（UTF-8）
        /// </summary>
        /// <param name="path">文件路径</param>
        /// <returns></returns>
        public static string GetTxt(string path)
        {
            StreamReader sr = new StreamReader(path, Encoding.UTF8);
            string str = sr.ReadToEnd();
            sr.Close();
            sr.Dispose();
            return str;
        }

        /// <summary>
        /// 将对象转换成字符串类型
        /// </summary>
        /// <param name="obj">传入的对象</param>
        /// <returns></returns>
        public static string ObjectToString(object obj)
        {
            var json = Newtonsoft.Json.JsonConvert.SerializeObject(obj);
            return json.ToString();
        }

        /// <summary>
        /// 取得标准化的当前时间（yyyy-MM-dd HH:mm:ss.fff）
        /// </summary>
        /// <returns></returns>
        public static DateTime GetNowDate()
        {
            return DateTime.Parse(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
        }

        /// <summary>
        /// MD5加密
        /// </summary>
        /// <param name="str">待加密的字符串</param>
        /// <returns></returns>
        public static string MD5(string str)
        {
            return Helper.MD5(str);
        }

        /// <summary>
        /// 发送post请求
        /// </summary>
        /// <param name="Url">请求路径</param>
        /// <param name="postDataStr">要提交的参数字符串</param>
        /// <returns></returns>
        public static string HttpPost(string Url, string postDataStr)//发送post请求
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(Url);
            request.Method = "POST";
            request.ContentType = "application/x-www-form-urlencoded";
            request.ContentLength = postDataStr.Length;
            StreamWriter writer = new StreamWriter(request.GetRequestStream(), Encoding.ASCII);
            writer.Write(postDataStr);
            writer.Flush();
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            string encoding = response.ContentEncoding;
            if (encoding == null || encoding.Length < 1)
            {
                encoding = "UTF-8"; //默认编码
            }
            StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.GetEncoding(encoding));
            string retString = reader.ReadToEnd();
            return retString;
        }

        /// <summary>
        /// 发送post请求
        /// </summary>
        /// <param name="Url">请求路径</param>
        /// <param name="postDataStr">要提交的参数字符串</param>
        /// <param name="headName">携带的头部名</param>
        /// <param name="headValue">携带的头部信息</param>
        /// <returns></returns>
        public static string HttpPost(string Url, string postDataStr, string headName, string headValue)//发送post请求
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(Url);
            request.Method = "POST";
            request.ContentType = "application/x-www-form-urlencoded";
            request.ContentLength = postDataStr.Length;
            request.Headers.Add(headName, headValue); //添加header
            StreamWriter writer = new StreamWriter(request.GetRequestStream(), Encoding.ASCII);
            writer.Write(postDataStr);
            writer.Flush();
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            string encoding = response.ContentEncoding;
            if (encoding == null || encoding.Length < 1)
            {
                encoding = "UTF-8"; //默认编码
            }
            StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.GetEncoding(encoding));
            string retString = reader.ReadToEnd();
            return retString;
        }

        /// <summary>
        /// 发送Get请求(返回字符串类型)
        /// </summary>
        /// <param name="Url">请求路径</param>
        /// <returns>返回字符串类型</returns>
        public static string HttpGet(string Url)//发送post请求
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(Url);
            request.Method = "GET";
            request.ContentType = "application/x-www-form-urlencoded";
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            string encoding = response.ContentEncoding;
            if (encoding == null || encoding.Length < 1)
            {
                encoding = "UTF-8"; //默认编码
            }
            StreamReader reader = new StreamReader(request.GetResponse().GetResponseStream(), Encoding.GetEncoding(encoding));
            string retString = reader.ReadToEnd();
            return retString;
        }

        /// <summary>
        /// 发送Get请求(返回一个stream流)
        /// </summary>
        /// <param name="Url">请求路径</param>
        /// <returns>返回一个stream流</returns>
        public static Stream GetHttpStream(string Url)//发送post请求
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(Url);
            request.Method = "GET";
            request.ContentType = "application/x-www-form-urlencoded";
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            string encoding = response.ContentEncoding;
            if (encoding == null || encoding.Length < 1)
            {
                encoding = "UTF-8"; //默认编码
            }
            return request.GetResponse().GetResponseStream();
        }

        /// <summary>
        /// 把一个字符串按标识拆分成多个字符串
        /// </summary>
        /// <param name="str">原字符串</param>
        /// <param name="idstr">截断标识</param>
        /// <returns></returns>
        public static List<string> Split(string str, string idstr) {
            List<string> result = new List<string>();
            //传入字符串不能为空
            if (string.IsNullOrEmpty(str)) {
                return new List<string>();
                //throw new Exception("传入的字符串为空");
            }
            //传入的标识为空
            if (string.IsNullOrEmpty(idstr)) {
                string[] strings = str.Split();
                result = strings.ToList();
                return result;
            }

            string tempStr = str;  //待处理字符串
            int idstrLength = idstr.Length;  //标识字符串的长度

            do {
                if (tempStr.IndexOf(idstr) == -1) { result.Add(tempStr); break; }  //如果找不到对应的标识,则把整个字符串添加进去
                int index = tempStr.IndexOf(idstr);      //取得标识字符串所在的位置
                result.Add(tempStr.Substring(0, index));    //截断字符串后添加到列表中
                tempStr = tempStr.Substring(index + idstrLength);   //更新待处理的字符串->去掉截走的部分以及标识部分
            } while (!string.IsNullOrEmpty(tempStr));  //如果待处理的字符串不为空则继续处理

            return result;
        }

        /// <summary>
        /// 把一个字符串按标识拆分成多个字符串
        /// </summary>
        /// <param name="str">原字符串</param>
        /// <param name="idstr">截断标识</param>
        /// <param name="isSaveIdstr">是否保留截断标识</param>
        /// <returns></returns>
        public static List<string> Split(string str, string idstr, bool isSaveIdstr)
        {
            if (string.IsNullOrEmpty(str)) {
                return new List<string>();
            }
            if (!isSaveIdstr) {
                return Split(str, idstr);
            }
            List<string> result = new List<string>();
            //传入字符串不能为空
            if (string.IsNullOrEmpty(str))
            {
                throw new Exception("传入的字符串为空");
            }
            //传入的标识为空
            if (string.IsNullOrEmpty(idstr))
            {
                string[] strings = str.Split();
                result = strings.ToList();
                return result;
            }

            string tempStr = str;  //待处理字符串
            int idstrLength = idstr.Length;  //标识字符串的长度

            do
            {
                if (tempStr.IndexOf(idstr) == -1) { result.Add(tempStr); break; }  //如果找不到对应的标识,则把整个字符串添加进去
                int index = tempStr.IndexOf(idstr);      //取得标识字符串所在的位置
                result.Add(tempStr.Substring(0, index));    //截断字符串后添加到列表中
                result.Add(tempStr.Substring(index, idstr.Length));  //将截断标识也存储进去
                tempStr = tempStr.Substring(index + idstrLength);   //更新待处理的字符串->去掉截走的部分以及标识部分
            } while (!string.IsNullOrEmpty(tempStr));  //如果待处理的字符串不为空则继续处理

            return result;
        }

        /// <summary>
        /// 字符串抓取（包含抓取规则中的字符）
        /// </summary>
        /// <param name="str">原始字符串</param>
        /// <param name="rule">抓取的字符串规则，例如ab{*}fg,其中'{*}'是自定义通配符，即后面的wildcard</param>
        /// <param name="wildcard">通配符</param>
        /// <returns>抓取到的字符串列表</returns>
        public static List<string> GrabString(string str, string rule, string wildcard)
        {
            List<string> strs = Helpers.Tool.Split(rule, wildcard, true); //把标识字符串按{*}拆分
            List<string> getStr = new List<string>();//存储抓取到的字符串
            string tempStr = str;
            do
            {
                string oneStr = "";  //记录字符串变量
                int beginPosition = 0;  //开始位置
                int endPosition = 0; //结束位置
                if (strs.Count == 1 && strs[0] == wildcard)
                {  //如果只输入了{*} 则直接抓取全部
                    oneStr = tempStr;
                }
                //遍历规则
                for (int i = 0; i < strs.Count; i++)
                {
                    if (i > 0)
                    {
                        if (strs[i - 1] == wildcard)//  如果匹配规则是通配符，由当前开始取，直到结束值
                        {
                            endPosition = tempStr.IndexOf(strs[i]); //取得单次结束的位置
                            if (endPosition == -1)  //如果找不到结尾的位置，则直接取到结尾
                            {
                                oneStr += tempStr;
                            }
                            else
                            {  //如果有结尾
                                endPosition += strs[i].Length;  //取结尾位置
                                oneStr += tempStr.Substring(beginPosition, endPosition);  //取字符串
                                beginPosition = endPosition;   //开始位置变更为结尾位置
                                endPosition = 0;  //结尾位置为初始
                                tempStr = tempStr.Substring(beginPosition);  //刷新检索字符串
                                beginPosition = 0;  //初始化起始位置
                            }
                        }
                    }
                    else
                    {
                        if (strs[i] == wildcard)  //如果以通配符开头，则直接跳过
                        { continue; }
                        else
                        {  //不以通配符开头
                            beginPosition = tempStr.IndexOf(strs[i]);  //找到第一个匹配的位置并记录
                            if (beginPosition == -1) { tempStr = null; break; }
                            oneStr += tempStr.Substring(beginPosition, strs[i].Length);  //添加到字符串里面
                            beginPosition += strs[i].Length;      //更新开始位置
                            tempStr = tempStr.Substring(beginPosition);  //更新字符串
                            beginPosition = 0;  //初始化起始位置
                        }
                    }
                }
                getStr.Add(oneStr);  //添加抓取结果
                oneStr = ""; //初始化记录字符串
            } while (!string.IsNullOrEmpty(tempStr));
            return getStr;
        }

        /// <summary>
        /// 字符串抓取
        /// </summary>
        /// <param name="str">原始字符串</param>
        /// <param name="rule">抓取的字符串规则，例如ab{*}fg,其中'{*}'是自定义通配符，即后面的wildcard</param>
        /// <param name="wildcard">通配符</param>
        /// <param name="isGrabRule">是否抓取规则中的字符</param>
        /// <returns>抓取到的字符串列表</returns>
        public static List<string> GrabString(string str, string rule, string wildcard, bool isGrabRule)
        {
            if (isGrabRule) { GrabString(str, rule, wildcard); }
            List<string> strs = Helpers.Tool.Split(rule, wildcard, true); //把标识字符串按{*}拆分
            List<string> getStr = new List<string>();//存储抓取到的字符串
            string tempStr = str;
            do
            {
                string oneStr = "";  //记录字符串变量
                int beginPosition = 0;  //开始位置
                int endPosition = 0; //结束位置
                if (strs.Count == 1 && strs[0] == wildcard)
                {  //如果只输入了{*} 则直接抓取全部
                    oneStr = tempStr;
                }
                //遍历规则
                for (int i = 0; i < strs.Count; i++)
                {
                    if (i > 0)
                    {
                        if (strs[i - 1] == wildcard)//  如果匹配规则是通配符，由当前开始取，直到结束值
                        {
                            endPosition = tempStr.IndexOf(strs[i]); //取得单次结束的位置
                            if (endPosition == -1)  //如果找不到结尾的位置，则直接取到结尾
                            {
                                oneStr += tempStr;
                            }
                            else
                            {  //如果有结尾
                                //endPosition += strs[i].Length;  //取结尾位置
                                oneStr += tempStr.Substring(beginPosition, endPosition);  //取字符串
                                beginPosition = endPosition;   //开始位置变更为结尾位置
                                endPosition = 0;  //结尾位置为初始
                                tempStr = tempStr.Substring(beginPosition);  //刷新检索字符串
                                beginPosition = 0;  //初始化起始位置
                            }
                        }
                    }
                    else
                    {
                        if (strs[i] == wildcard)  //如果以通配符开头，则直接跳过
                        { continue; }
                        else
                        {  //不以通配符开头
                            beginPosition = tempStr.IndexOf(strs[i]);  //找到第一个匹配的位置并记录
                            if (beginPosition == -1) { tempStr = null; break; }
                            //oneStr += tempStr.Substring(beginPosition, strs[i].Length);  //添加到字符串里面
                            beginPosition += strs[i].Length;      //更新开始位置
                            tempStr = tempStr.Substring(beginPosition);  //更新字符串
                            beginPosition = 0;  //初始化起始位置
                        }
                    }
                }
                if (!string.IsNullOrEmpty(oneStr)) {
                    getStr.Add(oneStr);
                }//添加抓取结果
                oneStr = ""; //初始化记录字符串
            } while (!string.IsNullOrEmpty(tempStr));
            return getStr;
        }

        /// <summary>
        /// 字符串内替换
        /// </summary>
        /// <param name="str">待处理的字符串</param>
        /// <param name="oldStr">要替换的内容</param>
        /// <param name="newStr">替换成的内容</param>
        /// <returns></returns>
        public static string ReplaceString(string str, string oldStr, string newStr) {
            if (string.IsNullOrEmpty(str)) { return ""; }
            string result = "";
            List<string> splitStr = Split(str, oldStr, true);  //按替换的串分割字符串
            for (int i = 0; i < splitStr.Count; i++)
            {
                if (splitStr[i] == oldStr)
                {
                    splitStr[i] = newStr;
                }
                result += splitStr[i];
            }
            return result;
        }

        /// <summary>
        /// 取得配置文件中的appsettings里面的值
        /// </summary>
        /// <returns></returns>
        public static string GetAppSetting(string key)
        {
            return System.Configuration.ConfigurationManager.AppSettings[key];
        }

        /// <summary>
        /// 检测时间的合法性
        /// </summary>
        /// <param name="t">检测的时间</param>
        /// <returns></returns>
        public static bool ValidateDate(DateTime t) {
            DateTime min = DateTime.Parse("1753-01-01");
            DateTime max = DateTime.Parse("2999-12-31");
            if (t < min || t > max)
            {
                return false;
            }
            else { return true; }
        }

        /// <summary>
        /// 格式化时间yyyy-MM-dd,失败返回1970-01-01
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static DateTime FormatDate(DateTime t) {
            DateTime result = DateTime.Now;
            bool tryResult = DateTime.TryParse(t.ToString("yyyy-MM-dd HH:mm:ss.fff"), out result);
            if (tryResult)
            {
                return result;
            }
            else {
                return DateTime.Parse("1970-01-01");
            }

        }

        /// <summary>
        /// 取得随机字符
        /// </summary>
        /// <param name="length"></param>
        /// <returns></returns>
        public static string GetRandStr(int length) {
            if (length < 1) { throw new Exception("长度必须大于0"); }
            try
            {
                string str = "";
                Random r = new Random();
                int i = 0;
                while (i < length)
                {
                    int num = r.Next(48, 122);
                    if (num > 47 && num < 58)
                    {
                        str += num.ToString();
                        i++;
                    }
                    else if (num > 64 && num < 91)
                    {
                        str += Convert.ToChar(num).ToString();
                        i++;
                    }
                    else if (num > 96 && num < 123)
                    {
                        str += Convert.ToChar(num).ToString();
                        i++;
                    }
                    else { continue; }
                }
                return str;
            }
            catch {
                return null;
            }
        }

        /// <summary>
        /// 删除指定文件
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static bool DeleteFile(string path) {
            try
            {
                System.IO.File.Delete(path);
                return true;
            }
            catch {
                return false;
            }
        }

        /// <summary>
        /// 创建全路径目录
        /// </summary>
        /// <param name="path"></param>
        public static void CreateLogDirectory(string path) {
            if (string.IsNullOrEmpty(path))
            {
                return;
            }
            string[] paths = path.Split('\\');
            string tempPath = "";
            foreach (var a in paths)
            {
                tempPath += a + "\\";
                Helper.CreateDirectory(tempPath, "");  //目录不存在的话创建目录
            }
        }

        /// <summary>
        /// 判断传入参数是否为空
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        public static bool IsNull(params object[] param) {
            foreach (var a in param) {
                if (a == null) {
                    return true;
                }
                if (string.IsNullOrEmpty(a.ToString())) {
                    return true;
                }
            }
            return false;
        }
    }
}
