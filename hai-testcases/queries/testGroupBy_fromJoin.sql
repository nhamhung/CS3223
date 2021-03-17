SELECT Customer.gender
FROM Customer,Cart
WHERE Customer.cid=Cart.cid
GROUPBY Customer.gender,Cart.cid