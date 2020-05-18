CREATE DATABASE IF NOT EXISTS companydb;
USE companydb;

CREATE TABLE  `companydb`.`clientes` (
	`ClienteID` int(3) NOT NULL AUTO_INCREMENT,
	`Nome` varchar(60) NOT NULL,
	`Estado_civil` varchar(30) NOT NULL,
	`Email` varchar(30),
	`Start_date` DATE NOT NULL,
    `Final_date` DATE NOT NULL,
	`Situation` varchar(20) NOT NULL,
	PRIMARY KEY (`ClienteID`)
) ENGINE=InnoDB AUTO_INCREMENT=32522 DEFAULT CHARSET=utf8mb4;

INSERT INTO `companydb`.`clientes` VALUES  (1001,'Joana','Casada','joana@email.com','2010-01-04', '9999-12-31','ativo'),
 (1002,'João','Solteiro','joao@email.com','2015-04-20', '9999-12-31', 'ativo'),
 (1003,'Ricardo','Solteiro','ricardo@email.com','2012-10-01', '9999-12-31', 'ativo'),
 (1004,'Gabriela','Solteira','gabriela@email.com','2014-08-06', '9999-12-31', 'ativo');
