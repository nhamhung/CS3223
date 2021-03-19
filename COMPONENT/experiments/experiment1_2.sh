cd tables || { echo "Error cd into tables"; exit 1;}
echo "=================================="
echo "========= STARTING BNL ==========="
echo "=================================="

echo "JOINING FLIGHTS AND SCHEDULE"
echo "buffer size: 2000 - buffer pages: 20"
echo "4000\n30\n1\n" | java QueryMain ../queries/joinFlightsAndSchedule.sql ../output/joinFlightsAndSchedule_4000-30.out

cd .. || { echo "Error cd into tables"; exit 1;}