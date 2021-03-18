cd tables || { echo "Error cd into tables"; exit 1;}
echo "=================================="
echo "========= STARTING BNL ==========="
echo "=================================="

echo "JOINING EMPLOYEES AND CERTIFIED"
echo "buffer size: 200 - buffer pages: 3"
echo "200\n3\n1\n" | java QueryMain ../queries/joinEmployeesAndCertified.sql ../output/joinEmployeesAndCertifiedFlipped_200-3.out
echo "----------------------------------"
echo "buffer size: 10000 - buffer pages: 3"
echo "10000\n3\n1\n" | java QueryMain ../queries/joinEmployeesAndCertified.sql ../output/joinEmployeesAndCertifiedFlipped_10000-3.out
echo "----------------------------------"
echo "buffer size: 4000 - buffer pages: 30"
echo "4000\n30\n1\n" | java QueryMain ../queries/joinEmployeesAndCertified.sql ../output/joinEmployeesAndCertified-4000-30.out
echo "----------------------------------"
echo "buffer size: 200 - buffer pages: 200"
echo "200\n200\n1\n" | java QueryMain ../queries/joinEmployeesAndCertified.sql ../output/joinEmployeesAndCertifiedFlipped_200-200.out
echo "----------------------------------"
echo "buffer size: 10000 - buffer pages: 200"
echo "10000\n200\n1\n" | java QueryMain ../queries/joinEmployeesAndCertified.sql ../output/joinEmployeesAndCertifiedFlipped_10000-200.out

cd .. || { echo "Error cd into tables"; exit 1;}