package com.mysite.applyhome.user;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Controller;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Controller
@RequestMapping("/user")
public class UserController {

	private final UserService userService;
//	private final PasswordEncoder passwordEncoder;

//	@GetMapping("/signup")
//	public String signup(UserCreateForm userCreateForm) {
//		return "signup_form";
//	}

//	@GetMapping("/asdf")
//	@ResponseBody
//	public String asdf(){
//		return passwordEncoder.encode("persona2");
//	}

	@PostMapping("/signup")
	public String signup(@ModelAttribute UserCreateForm userCreateForm) {
//		if (bindingResult.hasErrors()) {
//			return "signup_form";
//		}
//
//		if (!userCreateForm.getPassword1().equals(userCreateForm.getPassword2())) {
//			bindingResult.rejectValue("password2", "passwordInCorrect", "2개의 패스워드가 일치하지 않습니다.");
//			return "signup_form";
//		}
		System.out.println("프린트");
		try {
			System.out.println(userCreateForm.getUsername());
			userService.create(userCreateForm.getUsername(),
					userCreateForm.getResidentId(), userCreateForm.getPasswordConfirm());
			System.out.println("프린트2");
		} catch (DataIntegrityViolationException e) {
			e.printStackTrace();
			System.out.println(e);
//			bindingResult.reject("signupFailed", "이미 등록된 사용자입니다.");
			return "signup_form";
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);
//			bindingResult.reject("signupFailed", e.getMessage());
			return "signup_form";
		}

		return "redirect:/";
	}

//	@PostMapping("/signup")
//	public String signup(@Valid UserCreateForm userCreateForm, BindingResult bindingResult) {
//		if (bindingResult.hasErrors()) {
//			return "signup_form";
//		}
//
//		if (!userCreateForm.getPassword1().equals(userCreateForm.getPassword2())) {
//			bindingResult.rejectValue("password2", "passwordInCorrect", "2개의 패스워드가 일치하지 않습니다.");
//			return "signup_form";
//		}
//
//		try {
//			userService.create(userCreateForm.getUsername(),
//					userCreateForm.getResidentId(), userCreateForm.getPassword1());
//		} catch (DataIntegrityViolationException e) {
//			e.printStackTrace();
//			bindingResult.reject("signupFailed", "이미 등록된 사용자입니다.");
//			return "signup_form";
//		} catch (Exception e) {
//			e.printStackTrace();
//			bindingResult.reject("signupFailed", e.getMessage());
//			return "signup_form";
//		}
//
//		return "redirect:/";
//	}

	@GetMapping("/login")
	public String login() {
		return "login_form";
	}
}
