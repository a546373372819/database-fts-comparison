using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FTS_Test.Models.DB
{
    public class InsertObject
    {
        public long Gid { get; set; }
        public InsertValue[] Values { get; set; }
    }
}
