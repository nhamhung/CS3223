SELECT Customer.cid, Customer.firstname, Customer.gender, Cart.cartid, Cart.status
FROM Customer,Cart
WHERE Customer.cid=Cart.cid