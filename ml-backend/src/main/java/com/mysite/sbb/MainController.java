package com.mysite.sbb;

import com.mysite.sbb.question.Question;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class MainController {

	@GetMapping("/")
	public String root() {
		return "redirect:/question/list";
	}

	@GetMapping("/map")
	public String map() {
		return "map_page";
	}

	@GetMapping("/chatbot")
	public String chatbot() {
		return "chatbot";
	}
}
