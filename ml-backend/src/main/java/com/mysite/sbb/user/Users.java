package com.mysite.sbb.user;

import com.mysite.sbb.personalProfiles.PersonalProfiles;
import jakarta.persistence.*;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
public class Users {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

//	@Column(unique = true)
	private String userLoginId;

	private String userPasswordHash;

//	@Column(length = 13)
//	@Column(unique = true)
//	private String userRegistrationNumber;

	@OneToOne
	@JoinColumn(name = "userRegistrationNumber", referencedColumnName = "resident_registration_number")
	private PersonalProfiles personalProfiles;

//	@Column(unique = true)
//	private String email;
}
