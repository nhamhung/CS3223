SELECT Employees.eid,Certified.aid,Employees.ename,Employees.salary
FROM Certified,Employees
WHERE Employees.eid=Certified.eid