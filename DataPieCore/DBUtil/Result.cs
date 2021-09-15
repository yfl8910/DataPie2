namespace DBUtil
{
    /// <summary>
    /// 通用操作结果类
    /// </summary>
    public class Result
    {
        /// <summary>
        /// 是否成功
        /// </summary>
        public bool Success { set; get; }

        /// <summary>
        /// 返回结果(错误提示或数据)
        /// </summary>
        public object Data { set; get; }
    }
}