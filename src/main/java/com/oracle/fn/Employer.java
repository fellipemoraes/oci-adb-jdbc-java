package com.oracle.fn;

import com.opencsv.bean.CsvBindByName;

public class Employer {

    @CsvBindByName
    private String firstName;

    @CsvBindByName
    private String lastName;

    @CsvBindByName
    private int visitsToWebsite;

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public int getVisitsToWebsite() {
		return visitsToWebsite;
	}

	public void setVisitsToWebsite(int visitsToWebsite) {
		this.visitsToWebsite = visitsToWebsite;
	}

    
    
    
    }
