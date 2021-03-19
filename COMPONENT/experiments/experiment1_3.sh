cd tables || { echo "Error cd into tables"; exit 1;}
echo "=================================="
echo "========= STARTING BNL ==========="
echo "=================================="

echo "SCHEDULE AND AIRCRAFTS"
echo "buffer size: 4000 - buffer pages: 30"
echo "4000\n30\n1\n" | java QueryMain ../queries/joinScheduleAndAircrafts.sql ../output/joinScheduleAndAircrafts_4000-30.out

cd .. || { echo "Error cd into tables"; exit 1;}