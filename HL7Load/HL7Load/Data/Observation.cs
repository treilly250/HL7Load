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

        public int SequenceId { get; set; }

        public string CustomerId { get; set; }

        public string ObservationId { get; set; }

        public string ObservationIdText { get; set; }

        public string ResultData { get; set; }

        public string ResultUnits { get; set; }

        public string ReferenceRange { get; set; }

        public DateTime EntryDate { get; set; }

        public int TestId { get; set; }

        public string TestNormalRange { get; set; }

        public string TestNormalRangeFemale { get; set; }

        public int UserId { get; set; }

        public string UserCN { get; set; }

        public string InternalPatientId { get; set; }

        public string SSN { get; set; }

        public string LastName { get; set; }

        public string FirstName { get; set; }

        public string MiddleName { get; set; }

        public string DOB { get; set; }

        public string Gender { get; set; }

        public string StreetAddress { get; set; }

        public string OtherAddress { get; set; }

        public string City { get; set; }

        public string State { get; set; }

        public string Zip { get; set; }

        public int DataLoadMismatchTestID { get; set; }

        public int DataLoadValueErrorID { get; set; }

        public string ReplacementValue { get; set; }

        public Observation()
        {
        }
    }
}
