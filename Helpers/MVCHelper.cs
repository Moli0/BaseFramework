using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace Helpers
{
    public class MVCHelper
    {
        private HttpContextBase _HttpContext { get; set; }
        private HttpRequestBase _HttpRequest { get; set; }
        private HttpResponseBase _HttpResponse { get; set; }

        private MVCHelper() { }

        public MVCHelper(HttpContextBase httpContext) {
            _HttpContext = httpContext;
            _HttpRequest = _HttpContext.Request;
            _HttpResponse = _HttpContext.Response;
        }

        /// <summary>
        /// 设置本地cookie
        /// </summary>
        /// <param name="key">键</param>
        /// <param name="value">值</param>  
        /// <param name="minutes">过期时长，单位：分钟</param>      
        public void SetCookies(string key, string value, int minutes = 300,string path = "/",bool isDomain = false)
        {
            HttpCookie cookie = new HttpCookie(key, value);
            if (isDomain)
            {
                string host = Helpers.Tool.GetAppSetting("hostName");
                cookie.Domain = host;
            }
            cookie.Path = path;
            cookie.Expires = DateTime.Now.AddMinutes(minutes);
            _HttpResponse.Cookies.Add(cookie);
        }

        /// <summary>
        /// 设置只读cookie
        /// </summary>
        /// <param name="key">键</param>
        /// <param name="value">值</param>  
        /// <param name="minutes">过期时长，单位：分钟</param>  
        public void SetOnlyCookies(string key, string value, int minutes = 300, string path = "/", bool isDomain = false)
        {
            HttpCookie cookie = new HttpCookie(key, value);
            if (isDomain)
            {
                string host = Helpers.Tool.GetAppSetting("hostName");
                cookie.Domain = host;
            }
            cookie.Path = path;
            cookie.HttpOnly = true;
            cookie.Expires = DateTime.Now.AddMinutes(minutes);
            _HttpRequest.Cookies.Add(cookie);
        }


        /// <summary>
        /// 删除指定的cookie
        /// </summary>
        /// <param name="key">键</param>
        public void DeleteCookies(string key)
        {
            HttpCookie cookie = _HttpRequest.Cookies[key];
            if (cookie == null) {
                return;
            }
            cookie.Expires = DateTime.Now.AddDays(-1);
            _HttpResponse.AppendCookie(cookie);
        }

        /// <summary>
        /// 更新cookie
        /// </summary>
        /// <param name="key">键</param>
        /// <param name="value">值</param>  
        /// <param name="minutes">过期时长，单位：分钟</param>  
        public void UpdateCookies(string key, string value, int minutes = 300, string path = "/") {
            HttpCookie cookie = _HttpRequest.Cookies[key];
            if (cookie == null)
            {
                return;
            }
            cookie.Path = path;
            cookie.Value = value;
            cookie.Expires = DateTime.Now.AddMinutes(minutes);
            _HttpResponse.AppendCookie(cookie);
        }

        /// <summary>
        /// 获取cookies
        /// </summary>
        /// <param name="key">键</param>
        /// <returns>返回对应的值</returns>
        public string GetCookies(string key)
        {
            HttpCookie cookie = _HttpRequest.Cookies.Get(key);
            if (cookie == null)
            {
                return null;
            }
            string value = cookie.Value;
            if (string.IsNullOrEmpty(value))
                value = string.Empty;
            return value;
        }


    }
}
