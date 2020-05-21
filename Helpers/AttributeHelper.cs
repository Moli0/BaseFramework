using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Mvc;

namespace Helpers
{
    //public class UserLoginAttribute : ActionFilterAttribute, IAuthorizationFilter
    //{
    //    public bool IsLogin { get; set; }

    //    public UserLoginAttribute(bool isLogin = false)
    //    {
    //        IsLogin = isLogin;
    //    }

    //    public void OnAuthorization(AuthorizationContext filterContext)
    //    {
    //        MVCHelper helper = new MVCHelper(filterContext.RequestContext.HttpContext);
    //        string userid = helper.GetCookies("user");
    //        if (string.IsNullOrEmpty(userid))
    //        {
    //            filterContext.RequestContext.HttpContext.Response.StatusCode = 430;
    //            var content = new ContentResult();
    //            content.Content = "<script>window.top.location='/login';</script>";
    //            content.ContentEncoding = Encoding.UTF8;
    //            filterContext.Result = content;
    //            return;
    //        }
    //        else
    //        {
    //            DAL.UserInfoDAL dal = new DAL.UserInfoDAL();
    //            int userCount = dal.GetCount($"userid = '{userid}'");
    //            if (userCount == 0)
    //            {
    //                filterContext.RequestContext.HttpContext.Response.StatusCode = 430;
    //                var content = new ContentResult();
    //                content.Content = "<script>window.top.location='/login';</script>";
    //                content.ContentEncoding = Encoding.UTF8;
    //                filterContext.Result = content;
    //                return;
    //            }
    //            else
    //            {
    //                IsLogin = true;
    //            }
    //        }
    //    }
    //}
}
