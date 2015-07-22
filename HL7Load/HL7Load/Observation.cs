using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HL7Load
{
    class Observation
    {
        public string MessageId { get; set; }

        public string ObservationId { get; set; }

        public string ObservationIdText { get; set; }

        public string ResultData { get; set; }

        public string ResultUnits { get; set; }

        public string ReferenceRange { get; set; }

        public int TestId { get; set; }

        public Observation()
        {
        }
    }
}
