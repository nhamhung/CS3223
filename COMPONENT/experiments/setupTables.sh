cd tables || { echo "cd into tables/ failed"; exit 1; }
#java RandomDB Flights 10000
#java RandomDB Aircrafts 10000
#java RandomDB Schedule 10000
#java RandomDB Certified 10000
#java RandomDB Employees 10000
java ConvertTxtToTbl Flights
java ConvertTxtToTbl Aircrafts
java ConvertTxtToTbl Schedule
java ConvertTxtToTbl Certified
java ConvertTxtToTbl Employees
cd .. || { echo "cd .. failed"; exit 1; }
echo "successfully created tables"