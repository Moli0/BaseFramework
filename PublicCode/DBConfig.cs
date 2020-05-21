using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PublicCode
{
    /// <summary>
    /// 数据库连接配置类
    /// </summary>
    public class DBConfig
    {

        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        private string ConnectionStr { get; set; }
        /// <summary>
        /// 数据库服务器主机
        /// </summary>
        private string DBHost { get; set; }
        /// <summary>
        /// 数据库登录名
        /// </summary>
        private string ConnectionUserid { get; set; }
        /// <summary>
        /// 数据库登录密码
        /// </summary>
        private string ConnectionPwd { get; set; }
        /// <summary>
        /// 多数据库数据名称字典
        /// </summary>
        private Dictionary<string,string> ConnectionDBName { get; set; }
        /// <summary>
        /// 当前连接的数据库
        /// </summary>
        private string NowDBName { get; set; }
        /// <summary>
        /// 系统主数据库
        /// </summary>
        private string MainDBName { get; set; }

        /// <summary>
        /// 构造函数，构造时会初始化连接参数，而后可以调用InitConn返回打开的连接对象
        /// </summary>
        public DBConfig() {
            //以下配置建议写在配置文件中，再读取相应的配置文件
            DBHost = "";
            ConnectionUserid = "";
            ConnectionPwd = "";
            MainDBName = "";
            if (ConnectionDBName == null) {
                ConnectionDBName = new Dictionary<string, string>();
            }
        }

        /// <summary>
        /// 初始化数据库连接字符串
        /// </summary>
        /// <param name="type">配置中的数据库</param>
        /// <returns></returns>
        private void Init(string type)
        {
            if (string.IsNullOrWhiteSpace(type))
            {
                NowDBName = MainDBName;
            }
            else {
                ConnectionDBName.Remove(type);
                ConnectionDBName.Add(type, type);
                if (string.IsNullOrWhiteSpace(ConnectionDBName[type])) {
                    throw new Exception("未配置的数据库名称\r\nDatabase Name Is Invalid!");
                }
                NowDBName = ConnectionDBName[type];
            }
            ConnectionStr = $"server={DBHost};uid={ConnectionUserid};pwd={ConnectionPwd};database={NowDBName}";
        }

        /// <summary>
        /// 初始化数据库连接
        /// </summary>
        /// <param name="DBName">配置中的数据库</param>
        /// <returns>返回连接的数据库对象</returns>
        public SqlConnection InitConn(string DBName="")
        {
            Init(DBName);
            SqlConnection conn = new SqlConnection(ConnectionStr);
            conn.Open();
            return conn;
        }
    }
}
