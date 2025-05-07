package com.mysite.sbb.user;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UserCreateForm {
	@Size(min = 3, max = 25)
	@NotEmpty(message = "사용자 이름은 필수항목입니다.")
	private String username;

	@NotEmpty(message = "비밀번호는 필수항목입니다.")
	private String password1;

	@NotEmpty(message = "비밀번호 확인은 필수항목입니다.")
	private String password2;

//	@NotEmpty(message = "주민번호는 필수항목입니다.")
//	@Email
//	private String email;

	@NotEmpty(message = "주민번호는 필수항목입니다.")
	@Size(min = 12, max = 13, message = "주민번호는 12자리 이상 13자리 이하로 입력해야 합니다.")
	@Pattern(regexp = "^[0-9]+$", message = "주민번호는 숫자만 입력 가능합니다.")
	private String residentId;
}
