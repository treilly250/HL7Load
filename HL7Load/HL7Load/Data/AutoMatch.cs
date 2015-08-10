using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HL7Load
{
    class AutoMatch
    {
        public string LastName { get; set; }

        public string FirstName { get; set; }

        public string Gender { get; set; }

        public DateTime DOB { get; set; }

        public string SSN { get; set; }

        public int MatchedToUserId { get; set; }

        public AutoMatch()
        {
        }
    }
}
