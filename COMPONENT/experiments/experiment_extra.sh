cd tables || { echo "Error cd into tables"; exit 1;}

echo "5000\n100\n1\n" | java QueryMain ../queries/joinAircraftsFlightsAndSchedule.sql ../output/joinAircraftsFlightsAndSchedule.out # generic 115 values
echo "5000\n100\n1\n" | java QueryMain ../queries/distinctFlightNo_fomJoinAircraftsFlightsAndSchedule.sql ../output/distinctFlightNo_fomJoinAircraftsFlightsAndSchedule.out # generic 115 values
echo "8000\n60\n1\n" | java QueryMain ../queries/experiment_extra.sql ../output/experiment_extra.txt

cd .. || { echo "Error cd into tables"; exit 1;}