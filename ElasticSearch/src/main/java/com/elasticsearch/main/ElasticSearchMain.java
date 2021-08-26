package com.elasticsearch.main;


import com.elasticsearch.employeedao.EmployeeDao;

public class ElasticSearchMain {

    public static void main(String[] args) {
        EmployeeDao employeeDao = new EmployeeDao();
        //employeeDao.insertEmployee();
        employeeDao.getEmployee();

    }

}
