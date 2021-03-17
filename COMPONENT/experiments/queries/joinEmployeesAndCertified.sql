SELECT Employees.eid,Certified.aid,Employees.ename,Employees.salary
FROM Employees,Certified
WHERE Employees.eid=Certified.eid