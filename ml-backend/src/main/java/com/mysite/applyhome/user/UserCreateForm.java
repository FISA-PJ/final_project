package com.mysite.applyhome.user;

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
//	@Size(min = 12, max = 13, message = "주민번호는 12자리 이상 13자리 이하로 입력해야 합니다.")
//	@Pattern(regexp = "^[0-9]+$", message = "주민번호는 숫자만 입력 가능합니다.")
//	private String residentId;
	@NotEmpty(message = "주민번호 앞자리는 필수항목입니다.")
	@Size(min = 6, max = 6, message = "주민번호 앞자리는 6자리여야 합니다.")
	@Pattern(regexp = "^[0-9]{6}$", message = "주민번호 앞자리는 숫자 6자리여야 합니다.")
	private String residentId1;

	@NotEmpty(message = "주민번호 뒷자리는 필수항목입니다.")
	@Size(min = 7, max = 7, message = "주민번호 뒷자리는 7자리여야 합니다.")
	@Pattern(regexp = "^[0-9]{7}$", message = "주민번호 뒷자리는 숫자 7자리여야 합니다.")
	private String residentId2;

	public String getResidentId() {
		return residentId1 + residentId2;
	}
}
