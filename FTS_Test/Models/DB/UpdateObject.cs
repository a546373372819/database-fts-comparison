using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FTS_Test.Models.DB
{
    internal class UpdateObject
    {
        public long Gid { get; set; }
        public string[] ColumnNames { get; set; }
        public UpdateValue[] Values { get; set; }
    }
}
