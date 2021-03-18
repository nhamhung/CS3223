SELECT DISTINCT Customer.cid
FROM Customer,Cart
WHERE Customer.cid = Cart.cartid, Customer.cid <= Cart.cid, Customer.cid < Cart.cid, Cart.cid >= Customer.cid

