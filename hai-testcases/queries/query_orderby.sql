SELECT Customer.cid,Customer.gender
FROM Customer,Cart
WHERE Customer.cid=Cart.cid,Customer.cid>"150"
ORDERBY Customer.cid