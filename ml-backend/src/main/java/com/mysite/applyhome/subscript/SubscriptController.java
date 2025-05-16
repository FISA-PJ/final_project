package com.mysite.applyhome.subscript;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Controller
@RequestMapping("/subscript")
public class SubscriptController {
    @Value("${LH_API_KEY}")
    private String lhApiKey;

    @Autowired
    private SubscriptService subscriptService;

    @GetMapping({"", "/"})
    public String showSubscripts(
            Model model, 
            @RequestParam(value = "page", defaultValue = "1") int page,
            @RequestParam(value = "type", defaultValue = "05") String type
    ) {
        String serviceKey = lhApiKey;
        int pageSize = 10;
        Page<Subscript> paging = subscriptService.fetchSubscripts(serviceKey, pageSize, page, type);
        
        model.addAttribute("paging", paging);
        model.addAttribute("selectedType", type);
        return "subscript_list";
    }
}
