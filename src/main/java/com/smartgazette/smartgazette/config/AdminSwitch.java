package com.smartgazette.smartgazette.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

/**
 * The admin pages have no real login yet (Spring Security is the next roadmap
 * stage), so a public deployment runs read-only: with {@code admin.enabled=false}
 * (the default) every page that uploads, edits, deletes or posts answers 404.
 * New gazettes still arrive through the scheduled scraper.
 * Set {@code admin.enabled=true} only on a private machine.
 */
@Configuration
public class AdminSwitch implements WebMvcConfigurer {

    /** Paths that change data or expose admin tools. */
    static final List<String> ADMIN_PATHS = List.of(
            "/admin", "/admin/**", "/login", "/add", "/edit/**", "/update", "/delete/**", "/ifttt-post/**");

    @Value("${admin.enabled:false}")
    private boolean adminEnabled;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        if (adminEnabled) return;
        registry.addInterceptor(new HandlerInterceptor() {
            @Override
            public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
                    throws Exception {
                response.sendError(HttpServletResponse.SC_NOT_FOUND);
                return false;
            }
        }).addPathPatterns(ADMIN_PATHS);
    }
}
