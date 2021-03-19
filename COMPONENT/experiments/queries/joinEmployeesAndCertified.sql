SELECT Employees.eid,Employees.ename,Certified.eid,Certified.aid
FROM Certified,Employees
WHERE Employees.eid=Certified.eid
ORDERBY Employees.eid,Certified.aid