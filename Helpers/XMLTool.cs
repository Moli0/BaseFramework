using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace Helpers
{
    public static class XMLTool
    {
        /// <summary>
        /// 创建并加载XML文件，返回一个XML的对象
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static XmlDocument CreateXML(string path,string fileName) {
            string fullPath = string.Format(@"{0}\{1}.xml",path,fileName);
            try
            {
                Tool.CreateLogDirectory(path);
                if (!System.IO.Directory.Exists(path))
                {
                    Tool.PrintLog_Error("无法创建xml文件", string.Format("文件目录创建失败"));
                    //目录创建失败
                    return null;
                }
                //创建一个XML文件
                FileStream fs = File.Create(fullPath);
                XmlDocument xml = new XmlDocument();
                xml.Load(fullPath);
                return xml;
            }
            catch (Exception ex) {
                Tool.PrintLog_Error("无法创建xml文件", string.Format("“{0}”创建失败\r\n", fullPath) + ex.Message + ex.StackTrace);
                return null;
            }

        }

        /// <summary>
        /// 加载xml文件
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static XmlDocument XmlLoad(string path) {
            try
            {
                XmlDocument xml = new XmlDocument();
                xml.Load(path);
                return xml;
            }
            catch (Exception ex) {
                Tool.PrintLog_Error("xml文件加载失败", string.Format("“{0}”文件加载失败\r\n", path) + ex.Message + ex.StackTrace);
                return null;
            }
        }

    }
}
