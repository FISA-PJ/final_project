package com.mysite.applyhome;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class MainController {

	@Value("${KAKAOMAP_API_KEY}")
	private String kakaoApiKey;

	@GetMapping("/")
	public String root() {
		return "main_page";
	}

	@GetMapping("/desc")
	public String landing() {
		return "landing_page";
	}

	@GetMapping("/map")
	public String map(Model model) {
		model.addAttribute("kakaoApiKey", kakaoApiKey);
		return "map_page";
	}

	@GetMapping("/loan")
	public String loan(Model model) {
		return "loan_page";
	}

	@GetMapping("/mypage")
	public String mypage(Model model) {
		return "mypage";
	}
}
