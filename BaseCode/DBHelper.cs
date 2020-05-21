using Dapper;
using PublicCode;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BaseCode
{
    /// <summary>
    /// 数据库通用操作类
    /// 作者：LinMoli
    /// 最后更新时间：2020-05-20   已通过基础使用测试
    /// </summary>
    public static class DBHelper
    {
        /// <summary>
        /// 主数据库名称,在系统启动时设定
        /// </summary>
        public static string MainDBName = string.Empty;
        /// <summary>
        /// 默认跳过指定字段
        /// </summary>
        public static bool _isSkip = true;
        /// <summary>
        /// 插入时默认跳过的列，当isSkip为空或为true时有效
        /// </summary>
        public static string[] _InsertSkipColumns = new string[] { "create_time", "create_date", "create_datestr", "create_timestr" };
        /// <summary>
        /// 查询操作的默认排序方式，建议在系统启动时设置
        /// </summary>
        public static string DefaultSortStr = " id desc ";


        #region  插入操作
        /// <summary>
        /// 将实体插入到表中
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="Model">实体对象</param>
        /// <param name="tableName">表名，空值将以类型名作为表名</param>
        /// <param name="isSkip">是否包含跳过的含默认值的列，默认为true</param>
        /// <param name="skipColumns">当isSkip值为true时，不设置将以默认配置值为准</param>
        /// <returns></returns>
        public static int InsertSql<T>(T model,string tableName = "",bool? isSkip = null, string[] skipColumns = null) {
            if (string.IsNullOrWhiteSpace(MainDBName))
            {
                throw new Exception("数据库连接失败");
            }
            return InsertSql<T>(model, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), tableName, isSkip, skipColumns);
        }

        /// <summary>
        /// 将实体插入到指定的数据库中
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="model">实体对象</param>
        /// <param name="DBName">数据库名称</param>
        /// <param name="tableName">表名，可空，空时为类型名</param>
        /// <param name="isSkip">是否跳过指定列，默认跳过</param>
        /// <param name="skipColumns">跳过的列名，默认为配置的列</param>
        /// <returns></returns>
        public static int InsertSql<T>(T model, DBConfigEnum DBName,string tableName = "", bool? isSkip = null, string[] skipColumns = null) {
            StringBuilder sql = new StringBuilder();
            string columns = "";
            string values = "";
            DynamicParameters param = new DynamicParameters();

            if (isSkip == null)
            {
                isSkip = _isSkip;
            }
            if (skipColumns == null)
            {
                skipColumns = _InsertSkipColumns;
            }
            if (string.IsNullOrWhiteSpace(tableName))
            {
                tableName = typeof(T).Name;//model.GetType().Name;
            }

            foreach (var a in model.GetType().GetProperties())
            {
                if (isSkip == true)
                {
                    bool isContinue = false;
                    foreach (var c in skipColumns)
                    {
                        if (a.Name.ToLower() == c.ToLower())
                        {
                            isContinue = true;
                            break;
                        }
                    }
                    if (isContinue)
                    {
                        continue;
                    }
                }
                columns += a.Name + ",";
                values += "@" + a.Name + ",";
                if (string.IsNullOrWhiteSpace(Convert.ToString(a.GetValue(model)))) {
                    param.Add("@" + a.Name, null);
                }
                else
                {
                    param.Add(a.Name, a.GetValue(model).ToString());
                }
            }
            if (!string.IsNullOrWhiteSpace(columns))
            {
                columns = columns.Substring(0, columns.Length - 1);
                values = values.Substring(0, values.Length - 1);
            }
            sql.Append($" insert into {tableName}({columns}) values({values}) ");
            using (var conn = new DBConfig().InitConn(DBName.ToString()))
            {
                return conn.Execute(sql.ToString(), param);
            }
        }

        /// <summary>
        /// 将实体插入到表中
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="models">实体对象列表</param>
        /// <param name="tableName">表名，空值将以类型名作为表名</param>
        /// <param name="isSkip">是否包含跳过的含默认值的列，默认为true</param>
        /// <param name="skipColumns">当isSkip值为true时，不设置将以默认配置值为准</param>
        /// <returns></returns>
        public static int InsertSql<T>(List<T> models, string tableName = "", bool? isSkip = null, string[] skipColumns = null) {
            if (string.IsNullOrWhiteSpace(MainDBName))
            {
                throw new Exception("数据库连接失败");
            }
            return InsertSql<T>(models, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), tableName, isSkip, skipColumns);
        }

        /// <summary>
        /// 将实体插入到指定的数据库中
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="models">实体对象列表</param>
        /// <param name="DBName">数据库名称</param>
        /// <param name="tableName">表名，可空，空时为类型名</param>
        /// <param name="isSkip">是否跳过指定列，默认跳过</param>
        /// <param name="skipColumns">跳过的列名，默认为配置的列</param>
        /// <returns></returns>
        public static int InsertSql<T>(List<T> models, DBConfigEnum DBName, string tableName = "", bool? isSkip = null, string[] skipColumns = null) {
            StringBuilder sql = new StringBuilder();
            string columns = "";
            string values = "";
            string valueStr = "";

            if (models == null || models.Count == 0) {
                return 0;
            }

            if (isSkip == null)
            {
                isSkip = _isSkip;
            }
            if (skipColumns == null)
            {
                skipColumns = _InsertSkipColumns;
            }
            if (string.IsNullOrWhiteSpace(tableName))
            {
                tableName = models[0].GetType().Name;
            }

            foreach (var a in models[0].GetType().GetProperties()) {
                if (isSkip == true)
                {
                    bool isContinue = false;
                    foreach (var c in skipColumns)
                    {
                        if (a.Name.ToLower() == c.ToLower())
                        {
                            isContinue = true;
                            break;
                        }
                    }
                    if (isContinue)
                    {
                        continue;
                    }
                }
                columns += a.Name + ",";
            }

            foreach (var model in models) {
                values = "";
                foreach (var a in model.GetType().GetProperties()) {
                    if (isSkip == true)
                    {
                        bool isContinue = false;
                        foreach (var c in skipColumns)
                        {
                            if (a.Name.ToLower() == c.ToLower())
                            {
                                isContinue = true;
                                break;
                            }
                        }
                        if (isContinue)
                        {
                            continue;
                        }
                    }
                    if (string.IsNullOrWhiteSpace(Convert.ToString(a.GetValue(model))))
                    {
                        values += "NULL,";
                    }
                    else {
                        values += $"'{a.GetValue(model).ToString()}',";
                    }
                }
                if (!string.IsNullOrWhiteSpace(values))
                {
                    values = values.Substring(0, values.Length - 1);
                    valueStr += "\r\n select " + values + " union all ";
                }
            }
            if (!string.IsNullOrWhiteSpace(valueStr)) {
                valueStr = valueStr.Substring(0, valueStr.Length - (valueStr.Length - valueStr.LastIndexOf(" union all")));
            }
            if (!string.IsNullOrWhiteSpace(columns)) {
                columns = columns.Substring(0, columns.Length - 1);
            }
            sql.Append($" insert into {tableName}({columns}) {valueStr} ");

            using (var conn = new DBConfig().InitConn(DBName.ToString())) {
                return conn.Execute(sql.ToString());
            }

        }
        #endregion

        #region  更新操作
        /// <summary>
        /// 更新实体数据
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="model">实体</param>
        /// <param name="tableName">表名，可空，默认为实体名称</param>
        /// <param name="mainKey">主键列名，默认id</param>
        /// <returns></returns>
        public static int Update<T>(T model, string tableName= "", string mainKey = "id") {
            return Update<T>(model, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), tableName, mainKey);
        }

        /// <summary>
        /// 按数据库更新实体数据
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="model">实体</param>
        /// <param name="DBName">数据库名称</param>
        /// <param name="tableName">表名，可空，默认为实体名称</param>
        /// <param name="mainKey">主键列名，默认id</param>
        /// <returns></returns>
        public static int Update<T>(T model, DBConfigEnum DBName, string tableName = "", string mainKey = "id") {
            StringBuilder sql = new StringBuilder();
            string setStr = "";
            DynamicParameters param = new DynamicParameters();

            if (string.IsNullOrWhiteSpace(tableName)) {
                tableName = typeof(T).Name;
            }

            sql.Append($"update {tableName} set ");
            foreach (var a in model.GetType().GetProperties()) {
                if (string.IsNullOrWhiteSpace(Convert.ToString(a.GetValue(model)))) {
                    continue;
                }
                if (a.Name.ToLower() == mainKey.ToLower()) {
                    if (string.IsNullOrWhiteSpace(Convert.ToString(a.GetValue(model)))) {
                        return 0;
                    }
                    param.Add("@" + mainKey, a.GetValue(model).ToString());
                    continue;
                }
                setStr += $"{a.Name} = @{a.Name},";
                param.Add("@" + a.Name, a.GetValue(model).ToString());
            }

            if (!string.IsNullOrWhiteSpace(setStr)) {
                setStr = setStr.Substring(0, setStr.Length - 1);
                sql.Append(setStr);
                sql.Append($" where {mainKey} = @{mainKey} ");
                using (var conn = new DBConfig().InitConn(DBName.ToString())) {
                    return conn.Execute(sql.ToString(), param);
                }
            }
            else {
                return 0;
            }

        }
        /// <summary>
        /// 更新实体列表
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="models">实体列表</param>
        /// <param name="tableName">表名，可空，默认为实体名称</param>
        /// <param name="mainKey">主键列名，默认id</param>
        /// <returns></returns>
        public static int Update<T>(List<T> models, string tableName = "", string mainKey = "id") {
            return Update(models, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), tableName, mainKey,false);
        }

        /// <summary>
        /// 更新实体列表
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="models">实体列表</param>
        /// <param name="ErrorIsRollBack">错误是否回滚</param>
        /// <param name="tableName">表名，可空，默认为实体名称</param>
        /// <param name="mainKey">主键列名，默认id</param>
        /// <returns></returns>
        public static int Update<T>(List<T> models, bool ErrorIsRollBack, string tableName = "", string mainKey = "id")
        {
            if (string.IsNullOrWhiteSpace(MainDBName))
            {
                throw new Exception("数据库连接失败");
            }
            return Update(models, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), tableName, mainKey, ErrorIsRollBack);
        }

        /// <summary>
        /// 按数据库更新实体列表
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="models">实体列表</param>
        /// <param name="DBName">数据库名称</param>
        /// <param name="tableName">表名，可空，默认为实体名称</param>
        /// <param name="mainKey">主键列名，默认id</param>
        /// <param name="ErrorIsRollBack">错误是否回滚</param>
        /// <returns></returns>
        public static int Update<T>(List<T> models, DBConfigEnum DBName, string tableName = "", string mainKey = "id", bool ErrorIsRollBack = false) {

            int res = 0;
            if (string.IsNullOrWhiteSpace(tableName))
            {
                tableName = typeof(T).Name;
            }
            List<StringBuilder> sqls = new List<StringBuilder>();
            List<DynamicParameters> paramList = new List<DynamicParameters>();

            foreach (var model in models) {

                StringBuilder sql = new StringBuilder();
                string setStr = "";
                DynamicParameters param = new DynamicParameters();
                sql.Append($"update {tableName} set ");

                foreach (var a in model.GetType().GetProperties())
                {
                    if (string.IsNullOrWhiteSpace(Convert.ToString(a.GetValue(model))))
                    {
                        continue;
                    }
                    if (a.Name.ToLower() == mainKey.ToLower())
                    {
                        if (string.IsNullOrWhiteSpace(Convert.ToString(a.GetValue(model))))
                        {
                            return 0;
                        }
                        param.Add("@" + mainKey, a.GetValue(model).ToString());
                        continue;
                    }
                    setStr += $"{a.Name} = @{a.Name},";
                    param.Add("@" + a.Name, a.GetValue(model).ToString());
                }

                if (!string.IsNullOrWhiteSpace(setStr))
                {
                    setStr = setStr.Substring(0, setStr.Length - 1);
                    sql.Append(setStr);
                    sql.Append($" where {mainKey} = @{mainKey} ");
                    sqls.Add(sql);
                    paramList.Add(param);
                }

            }
            using (var conn = new DBConfig().InitConn(DBName.ToString())) {
                IDbTransaction tran = conn.BeginTransaction();
                for (int i = 0; i < sqls.Count; i++) {
                    res += conn.Execute(sqls[i].ToString(), paramList[i], tran);
                }
                if (ErrorIsRollBack) {
                    if (res < models.Count || res > models.Count) {
                        tran.Rollback();
                        return 0;
                    }
                }
                tran.Commit();
                return res;
            }
        }
        #endregion

        #region  删除操作
        /// <summary>
        /// 根据键值删除数据
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="id">键值</param>
        /// <param name="tableName">表名，默认为实体类型名称</param>
        /// <param name="mainKey">列名，默认id</param>
        /// <returns></returns>
        public static int Delete<T>(string id, string tableName = "", string mainKey = "id") {
            if (string.IsNullOrWhiteSpace(MainDBName)) {
                throw new Exception("数据库连接失败");
            }
            return Delete<T>(id, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), tableName, mainKey);
        }

        /// <summary>
        /// 按数据库根据键值删除数据
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="id">键值</param>
        /// <param name="DBName">数据库名称</param>
        /// <param name="tableName">表名，默认为实体类型名称</param>
        /// <param name="mainKey">列名，默认id</param>
        /// <returns></returns>
        public static int Delete<T>(string id, DBConfigEnum DBName, string tableName = "",string mainKey = "id") {
            if (string.IsNullOrWhiteSpace(tableName)) {
                tableName = typeof(T).Name;
            }
            if (string.IsNullOrWhiteSpace(id)||string.IsNullOrWhiteSpace(mainKey)) {
                return 0;
            }
            string sql = $" Delete {tableName} where {mainKey} = @{mainKey}";
            DynamicParameters param = new DynamicParameters();
            param.Add("@" + mainKey, id);
            using (var conn = new DBConfig().InitConn(DBName.ToString())) {
                return conn.Execute(sql, param);
            }
        }

        /// <summary>
        /// 根据键值删除数据
        /// </summary>
        /// <param name="id">键值</param>
        /// <param name="tableName">表名</param>
        /// <param name="mainKey">列名，默认id</param>
        /// <returns></returns>
        public static int Delete(string id, string tableName, string mainKey = "id") {
            if (string.IsNullOrWhiteSpace(MainDBName))
            {
                throw new Exception("数据库连接失败");
            }
            return Delete(id, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), tableName, mainKey);
        }

        /// <summary>
        /// 按数据库根据键值删除数据
        /// </summary>
        /// <param name="id">键值</param>
        /// <param name="DBName">数据库名称</param>
        /// <param name="tableName">表名</param>
        /// <param name="mainKey">列名，默认id</param>
        /// <returns></returns>
        public static int Delete(string id, DBConfigEnum DBName, string tableName, string mainKey = "id") {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new Exception("无效的表名");
            }
            if (string.IsNullOrWhiteSpace(id) || string.IsNullOrWhiteSpace(mainKey))
            {
                return 0;
            }
            string sql = $" Delete {tableName} where {mainKey} = @{mainKey}";
            DynamicParameters param = new DynamicParameters();
            param.Add("@" + mainKey, id);
            using (var conn = new DBConfig().InitConn(DBName.ToString()))
            {
                return conn.Execute(sql, param);
            }
        }

        /// <summary>
        /// 使用事务删除多条数据
        /// </summary>
        /// <param name="ids">键值</param>
        /// <param name="tableName">表名</param>
        /// <param name="mainKey">主键</param>
        /// <param name="ErrorIsRollBack">是否需要匹配传入的列表行数，当行数与变更行数不等时回滚，默认不需要</param>
        /// <returns></returns>
        public static int Delete(List<string> ids, string tableName, string mainKey = "id", bool ErrorIsRollBack = false) {
            if (string.IsNullOrWhiteSpace(MainDBName))
            {
                throw new Exception("数据库连接失败");
            }
            return Delete(ids, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), tableName, mainKey, ErrorIsRollBack);
        }

        /// <summary>
        /// 按数据库使用事务删除多条数据
        /// </summary>
        /// <param name="ids">键值</param>
        /// <param name="DBName">数据库名称</param>
        /// <param name="tableName">表名</param>
        /// <param name="mainKey">主键</param>
        /// <param name="ErrorIsRollBack">是否需要匹配传入的列表行数，当行数与变更行数不等时回滚，默认不需要</param>
        /// <returns></returns>
        public static int Delete(List<string> ids, DBConfigEnum DBName, string tableName, string mainKey = "id", bool ErrorIsRollBack = false) {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new Exception("无效的表名");
            }
            int res = 0;
            using (var conn = new DBConfig().InitConn(DBName.ToString())) {

                IDbTransaction tran = conn.BeginTransaction();
                try
                {
                    foreach (var id in ids)
                    {
                        if (string.IsNullOrWhiteSpace(id) || string.IsNullOrWhiteSpace(mainKey))
                        {
                            tran.Rollback();
                            return 0;
                        }
                        string sql = $" Delete {tableName} where {mainKey} = @{mainKey}";
                        DynamicParameters param = new DynamicParameters();
                        param.Add("@" + mainKey, id);
                        res += conn.Execute(sql, param,tran);
                    }
                    if (ErrorIsRollBack)
                    {
                        if (res < ids.Count || res > ids.Count)
                        {
                            tran.Rollback();
                            return 0;
                        }
                    }
                    tran.Commit();
                    return res;
                }
                catch {
                    tran.Rollback();
                    return 0;
                }
            }
        }

        /// <summary>
        /// 使用事务删除多个表的数据
        /// </summary>
        /// <param name="list">
        /// Hashtable格式：
        /// hs["id"]  键值
        /// hs["table"]  表名
        /// hs["mainKey"] 列名
        /// </param>
        /// <param name="mainKey">默认列名</param>
        /// /// <param name="ErrorIsRollBack">是否需要匹配传入的列表行数，当行数与变更行数不等时回滚，默认不需要</param>
        /// <returns></returns>
        public static int Delete(List<Hashtable> list,string mainKey = "id", bool ErrorIsRollBack = false)
        {
            if (string.IsNullOrWhiteSpace(MainDBName))
            {
                throw new Exception("数据库连接失败");
            }
            return Delete(list, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), mainKey, ErrorIsRollBack);
        }

        /// <summary>
        /// 按数据库使用事务删除多个表的数据
        /// </summary>
        /// <param name="list">
        /// Hashtable格式：
        /// hs["id"]  键值
        /// hs["table"]  表名
        /// hs["mainKey"] 列名
        /// </param>
        /// <param name="DBName">数据库名称</param>
        /// <param name="mainKey">默认列名</param>
        /// <param name="ErrorIsRollBack">是否需要匹配传入的列表行数，当行数与变更行数不等时回滚，默认不需要</param>
        /// <returns></returns>
        public static int Delete(List<Hashtable> list, DBConfigEnum DBName, string mainKey = "id", bool ErrorIsRollBack = false)
        {
            int res = 0;
            using (var conn = new DBConfig().InitConn(DBName.ToString()))
            {

                IDbTransaction tran = conn.BeginTransaction();
                try
                {
                    foreach (var hs in list)
                    {
                        if (string.IsNullOrWhiteSpace(Convert.ToString(hs["id"])) || string.IsNullOrWhiteSpace(Convert.ToString(hs["table"])))
                        {
                            tran.Rollback();
                            return 0;
                        }
                        if (string.IsNullOrWhiteSpace(Convert.ToString(hs["mainKey"]))) {
                            hs["mainKey"] = mainKey;
                        }
                        string sql = $" Delete {Convert.ToString(hs["table"])} where {Convert.ToString(hs["mainKey"])} = @{Convert.ToString(hs["mainKey"])}";
                        DynamicParameters param = new DynamicParameters();
                        param.Add("@" + Convert.ToString(hs["mainKey"]), Convert.ToString(hs["id"]));
                        res += conn.Execute(sql, param,tran);
                    }
                    if (ErrorIsRollBack)
                    {
                        if (res < list.Count || res > list.Count)
                        {
                            tran.Rollback();
                            return 0;
                        }
                    }
                    tran.Commit();
                    return res;
                }
                catch
                {
                    tran.Rollback();
                    return 0;
                }
            }
        }
        #endregion

        #region 数据查询

        /// <summary>
        /// 执行SQL返回DataSet
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns></returns>
        public static DataSet ExecuteSql(string sql) {
            if (string.IsNullOrWhiteSpace(MainDBName))
            {
                throw new Exception("数据库连接失败");
            }
            return ExecuteSql(sql, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName));
        }

        /// <summary>
        /// 执行SQL返回DataSet
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="DBName">数据库名称</param>
        /// <returns></returns>
        public static DataSet ExecuteSql(string sql,DBConfigEnum DBName)
        {
            using (var conn = new DBConfig().InitConn(DBName.ToString()))
            {
                SqlCommand comm = new SqlCommand(sql, conn);
                SqlDataAdapter sda = new SqlDataAdapter(comm);
                DataSet ds = new DataSet();
                sda.Fill(ds);
                return ds;
            }
        }

        #region  快速查询

        #region 单表查询
        /// <summary>
        /// 根据键值取得一个实体
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="id">键值</param>
        /// <param name="tableName">表名，默认为实体类型名</param>
        /// <param name="mainKey">键名，默认id</param>
        /// <param name="sortStr">排序方式，默认为系统设置的默认值</param>
        /// <returns></returns>
        public static T GetModel<T>(string id, string tableName = "", string mainKey = "id", string sortStr = "") {
            if (string.IsNullOrWhiteSpace(MainDBName))
            {
                throw new Exception("数据库连接失败");
            }
            return GetModel<T>(id, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), tableName, mainKey, sortStr);
        }

        /// <summary>
        /// 按数据库根据键值取得一个实体
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="id">键值</param>
        /// <param name="DBName">数据库名称</param>
        /// <param name="tableName">表名，默认为实体类型名</param>
        /// <param name="mainKey">键名，默认id</param>
        /// <param name="sortStr">排序方式，默认为系统设置的默认值</param>
        /// <returns></returns>
        public static T GetModel<T>(string id, DBConfigEnum DBName,string tableName = "", string mainKey = "id",string sortStr = "") {
            if (string.IsNullOrWhiteSpace(tableName)) {
                tableName = typeof(T).Name;
            }
            if (string.IsNullOrWhiteSpace(sortStr)) {
                sortStr = DefaultSortStr;
            }
            if (string.IsNullOrWhiteSpace(id) || string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(mainKey) || string.IsNullOrWhiteSpace(sortStr)) {
                throw new Exception("空值的参数设定");
            }
            string sql = $"select top 1 * from {tableName} where {mainKey} = @{mainKey} order by {sortStr} ";
            DynamicParameters param = new DynamicParameters();
            param.Add("@" + mainKey, id);
            using (var conn = new DBConfig().InitConn(DBName.ToString())) {
                return conn.Query<T>(sql, param).SingleOrDefault();
            }
        }
        /// <summary>
        /// 根据键值取得一个实体
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="cond">条件,请以and|or等连接字开头</param>
        /// <param name="tableName">表名，默认为实体类型名</param>
        /// <param name="mainKey">键名，默认id</param>
        /// <param name="sortStr">排序方式，默认为系统设置的默认值</param>
        /// <returns></returns>
        public static T GetModelForCond<T>(string cond, string tableName = "", string mainKey = "id", string sortStr = "") {
            if (string.IsNullOrWhiteSpace(MainDBName))
            {
                throw new Exception("数据库连接失败");
            }
            return GetModelForCond<T>(cond, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), tableName, sortStr);
        }
        /// <summary>
        /// 按数据库根据键值取得一个实体
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="cond">条件,请以and|or等连接字开头</param>
        /// <param name="DBName">数据库名称</param>
        /// <param name="tableName">表名，默认为实体类型名</param>
        /// <param name="sortStr">排序方式，默认为系统设置的默认值</param>
        public static T GetModelForCond<T>(string cond, DBConfigEnum DBName, string tableName = "", string sortStr = "")
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                tableName = typeof(T).Name;
            }
            if (string.IsNullOrWhiteSpace(sortStr))
            {
                sortStr = DefaultSortStr;
            }
            if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(sortStr))
            {
                throw new Exception("空值的参数设定");
            }
            string sql = $"select top 1 * from {tableName} where 1=1 {cond} order by {sortStr} ";
            using (var conn = new DBConfig().InitConn(DBName.ToString()))
            {
                return conn.Query<T>(sql).SingleOrDefault();
            }
        }
        /// <summary>
        /// 根据键值取得一个实体
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="cond">条件,请以and|or等连接字开头</param>
        /// <param name="tableName">表名，默认为实体类型名</param>
        /// <param name="sortStr">排序方式，默认为系统设置的默认值</param>
        /// <returns></returns>
        public static List<T> GetModels<T>(string cond, string tableName = "", string sortStr = "")
        {
            if (string.IsNullOrWhiteSpace(MainDBName))
            {
                throw new Exception("数据库连接失败");
            }
            return GetModels<T>(cond, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), tableName, sortStr);
        }
        /// <summary>
        /// 按数据库根据键值取得一个实体
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="cond">条件,请以and|or等连接字开头</param>
        /// <param name="DBName">数据库名称</param>
        /// <param name="tableName">表名，默认为实体类型名</param>
        /// <param name="sortStr">排序方式，默认为系统设置的默认值</param>
        public static List<T> GetModels<T>(string cond, DBConfigEnum DBName, string tableName = "", string sortStr = "")
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                tableName = typeof(T).Name;
            }
            if (string.IsNullOrWhiteSpace(sortStr))
            {
                sortStr = DefaultSortStr;
            }
            if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(sortStr))
            {
                throw new Exception("空值的参数设定");
            }
            string sql = $"select * from {tableName} where 1=1 {cond} order by {sortStr} ";
            using (var conn = new DBConfig().InitConn(DBName.ToString()))
            {
                return conn.Query<T>(sql).ToList();
            }
        }
        #endregion

        #region 多表查询
        /// <summary>
        /// 根据传入的参数进行查询
        /// </summary>
        /// <param name="columns">查询的列名</param>
        /// <param name="tableSql">查询的表体</param>
        /// <param name="cond">条件，以and|or连接字开始</param>
        /// <param name="sort">排序字段</param>
        /// <returns></returns>
        public static DataSet GetTable(string columns,string tableSql,string cond,string sort) {
            if (string.IsNullOrWhiteSpace(MainDBName))
            {
                throw new Exception("数据库连接失败");
            }
            return GetTable(columns, tableSql, cond, sort, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName));
        }
        /// <summary>
        /// 按数据库名称并根据传入的参数进行查询
        /// </summary>
        /// <param name="columns">查询的列名</param>
        /// <param name="tableSql">查询的表体</param>
        /// <param name="cond">条件，以and|or连接字开始</param>
        /// <param name="sort">排序字段</param>
        /// <param name="DBName">数据库名称</param>
        /// <returns>返回一个DataSet</returns>
        public static DataSet GetTable(string columns, string tableSql, string cond, string sort, DBConfigEnum DBName)
        {
            if (string.IsNullOrWhiteSpace(sort))
            {
                sort = DefaultSortStr;
            }
            if (string.IsNullOrWhiteSpace(columns) || string.IsNullOrWhiteSpace(tableSql) || string.IsNullOrWhiteSpace(sort))
            {
                throw new Exception("空值的参数设定");
            }
            string sql = $@"select * from ( 
	select ROW_NUMBER() over( order by {sort}) rowid, {columns} from {tableSql}
	where 1=1 {cond}
) t ";
            DataSet ds = new DataSet();
            ds = ExecuteSql(sql, DBName);
            return ds;
        }
        /// <summary>
        /// 根据传入的参数进行查询
        /// </summary>
        /// <param name="columns">查询的列名</param>
        /// <param name="tableSql">查询的表体</param>
        /// <param name="cond">条件，以and|or连接字开始</param>
        /// <param name="sort">排序字段</param>
        /// <param name="rowCount">查询结果的行数</param>
        /// <returns>返回一个DataSet并带出查询的行数</returns>
        public static DataSet GetTable(string columns, string tableSql, string cond, string sort,out int rowCount)
        {
            if (string.IsNullOrWhiteSpace(MainDBName))
            {
                throw new Exception("数据库连接失败");
            }
            return GetTable(columns, tableSql, cond, sort, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), out rowCount);
        }
        /// <summary>
        /// 按数据库名称并根据传入的参数进行查询
        /// </summary>
        /// <param name="columns">查询的列名</param>
        /// <param name="tableSql">查询的表体</param>
        /// <param name="cond">条件，以and|or连接字开始</param>
        /// <param name="sort">排序字段</param>
        /// <param name="DBName">数据库名称</param>
        /// <param name="rowCount">查询结果的行数</param>
        /// <returns>返回一个DataSet并带出查询的行数</returns>
        public static DataSet GetTable(string columns, string tableSql, string cond, string sort,DBConfigEnum DBName,out int rowCount)
        {
            if (string.IsNullOrWhiteSpace(sort)) {
                sort = DefaultSortStr;
            }
            if (string.IsNullOrWhiteSpace(columns) || string.IsNullOrWhiteSpace(tableSql) || string.IsNullOrWhiteSpace(sort)) {
                throw new Exception("空值的参数设定");
            }
            string sql = $@"select * from ( 
	select ROW_NUMBER() over( order by {sort}) rowid, {columns} from {tableSql}
	where 1=1 {cond}
) t ";
            string countSql = $@"select Count(1) from ( 
	select ROW_NUMBER() over( order by {sort}) rowid, {columns} from {tableSql}
	where 1=1 {cond}
) t ";
            DataSet ds = new DataSet();
            ds = ExecuteSql(sql, DBName);
            using (var conn = new DBConfig().InitConn(DBName.ToString())) {
                rowCount = conn.ExecuteScalar<int>(countSql);
            }
            return ds;
        }
        #endregion

        #endregion

        #region  分页数据查询 
        //分页查询需要传入的参数设定：当前页，单页行数
        //返回的参数设定：DataSet数据，总行数
        /// <summary>
        /// 分页取得数据
        /// </summary>
        /// <param name="page">分页实体</param>
        /// <param name="columns">查询的列</param>
        /// <param name="tableSql">查询的表体</param>
        /// <param name="cond">查询的条件</param>
        /// <param name="totalRows">OUT->总数据行数</param>
        /// <returns>返回查询出来的数据以及输入总数据行数</returns>
        public static DataSet GetTableForPagination(Pagination page, string columns, string tableSql, string cond, out int totalRows) {
            if (string.IsNullOrWhiteSpace(MainDBName))
            {
                throw new Exception("数据库连接失败");
            }
            return GetTableForPagination(page, columns, tableSql, cond, (DBConfigEnum)Enum.Parse(typeof(DBConfigEnum), MainDBName), out totalRows);
        }
        /// <summary>
        /// 按数据库取得分页取得数据
        /// </summary>
        /// <param name="page">分页实体</param>
        /// <param name="columns">查询的列</param>
        /// <param name="tableSql">查询的表体</param>
        /// <param name="cond">查询的条件</param>
        /// <param name="DBName">数据库名称</param>
        /// <param name="totalRows">OUT->总数据行数</param>
        /// <returns>返回查询出来的数据以及输入总数据行数</returns>
        public static DataSet GetTableForPagination(Pagination page,string columns,string tableSql,string cond, DBConfigEnum DBName, out int totalRows) {
            int pageIndex = page.NowPage;
            int pageSize = page.PageSize;
            totalRows = 0;
            if (string.IsNullOrWhiteSpace(page.Sort)) {
                page.Sort = DefaultSortStr;
                page.SortType = "";
            }
            else if (string.IsNullOrWhiteSpace(page.SortType)) {
                page.SortType = " asc ";
            }
            if (string.IsNullOrWhiteSpace(columns) || string.IsNullOrWhiteSpace(tableSql)) {
                throw new Exception("传入的列名与表名均不能为空");
            }
            string sql = $@"select * from (
	select ROW_NUMBER() OVER(order by {page.Sort} {page.SortType} ) rowid,{columns} from {tableSql} where 1=1 {cond}
) t where 1=1 and rowid between {((pageIndex - 1) * pageSize) + 1} and {pageIndex * pageSize}";
            string countSql = $@"select Count(1) counts from (
	select {columns} from {tableSql} where 1=1 {cond}
) t ";
            DataSet ds = ExecuteSql(sql, DBName);
            using (var conn = new DBConfig().InitConn(DBName.ToString())) {
                totalRows = conn.ExecuteScalar<int>(countSql);
            }
            return ds;
        }
        #endregion

        #region 统计查询
        #endregion

        #endregion

    }
}
