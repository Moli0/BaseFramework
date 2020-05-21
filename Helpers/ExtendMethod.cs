using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Helpers
{
    public static class ExtendMethod
    {
        public static string ToJson(this object obj) {
            if (obj == null) {
                return null;
            }
            return Newtonsoft.Json.JsonConvert.SerializeObject(obj);
        }

        public static T ToModel<T>(this object obj) {
            if (obj == null) {
                return default(T);
            }
            string json = Newtonsoft.Json.JsonConvert.SerializeObject(obj);
            T model = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
            return model;
        }

        public static DataTable ToTable(this object obj) {
            if (obj == null) {
                return null;
            }
            string json = Newtonsoft.Json.JsonConvert.SerializeObject(obj);
            return Newtonsoft.Json.JsonConvert.DeserializeObject<DataTable>(json);
        }

        public static int ToInt(this string str) {
            int resInt = 0;
            if (int.TryParse(str, out resInt))
            {
                return resInt;
            }
            else {
                throw new Exception("该字符串无法转换成整形");
            }
        }

        public static DateTime ToDateTime(this object obj) {
            if (obj == null) {
                throw new Exception("对象不能为空");
            }
            string str = obj.ToString();
            DateTime dt = DateTime.Now;
            if (DateTime.TryParse(str, out dt))
            {
                return dt;
            }
            else {
                throw new Exception("非法字符串");
            }
        }

        public static string ToDate(this object obj) {
            if (obj == null) {
                return null;
            }
            string str = obj.ToString();
            DateTime dt = DateTime.Now;
            if (DateTime.TryParse(str, out dt))
            {
                return dt.ToString("yyyy-MM-dd");
            }
            else {
                throw new Exception("无法转换的内容");
            }
        }
    }
}
