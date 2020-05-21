using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PublicCode
{
    public class Pagination
    {
        /// <summary>
        /// 当前页
        /// </summary>
        public int NowPage { get; set; }

        /// <summary>
        /// 单页行数 
        /// </summary>
        public int PageSize { get; set; }

        /// <summary>
        /// 排序字段
        /// </summary>
        public string Sort { get; set; }

        /// <summary>
        /// 排序类型  asc/desc
        /// </summary>
        public string SortType { get; set; }

        /// <summary>
        /// 数据总条数
        /// </summary>
        public int counts { get; set; }

        /// <summary>
        /// 页面总数
        /// </summary>
        public int PageTotle { get; set; }
    }
}
