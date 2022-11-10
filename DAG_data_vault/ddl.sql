CREATE TABLE if not exists stage.orders (
	orderid int4 NOT NULL,
	orderdate date NULL,
	orderstatus varchar(1) NULL,
	orderpriority varchar(15) NULL,
	clerk varchar(15) NULL,
	CONSTRAINT orders_pk PRIMARY KEY (orderid)
);

CREATE TABLE if not exists stage.products (
	productid int4 NOT NULL,
	productname varchar(55) NULL,
	producttype varchar(25) NULL,
	productsize int4 NULL,
	retailprice numeric(15,2) NULL,
	CONSTRAINT products_pk PRIMARY KEY (productid)
);

CREATE TABLE if not exists stage.suppliers (
	supplierid int4 NOT NULL,
	suppliername varchar(25) NULL,
	address varchar(40) NULL,
	phone varchar(15) NULL,
	balance numeric(15,2) NULL,
	descr varchar(101) NULL,
	CONSTRAINT supplier_pk PRIMARY KEY (supplierid)
);

CREATE TABLE if not exists stage.orderdetails (
	orderid int4 NOT NULL,
	productid int4 NOT NULL,
	unitprice numeric(15,2) NULL,
	quantity numeric(15,2) NULL,
	discount numeric(15,2) NULL,
	CONSTRAINT orderdetails_pk PRIMARY KEY (orderid, productid),
	CONSTRAINT orderdetails_fk FOREIGN KEY (orderid) REFERENCES stage.orders(orderid),
	CONSTRAINT orderdetails_fk_1 FOREIGN KEY (productid) REFERENCES stage.products(productid)
);

CREATE TABLE if not exists stage.productsuppl (
	productid int4 NOT NULL,
	supplierid int4 NOT NULL,
	qty int4 NULL,
	supplycost numeric(15,2) NULL,
	descr varchar(199) NULL,
	CONSTRAINT productsuppl_pk PRIMARY KEY (productid, supplierid),
	CONSTRAINT productsuppl_fk FOREIGN KEY (productid) REFERENCES stage.products(productid),
	CONSTRAINT productsuppl_fk_1 FOREIGN KEY (supplierid) REFERENCES stage.suppliers(supplierid)
);

