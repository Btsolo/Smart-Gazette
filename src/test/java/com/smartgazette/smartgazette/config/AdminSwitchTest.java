package com.smartgazette.smartgazette.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** A public deployment is read-only: admin.enabled=false hides every page that changes data.
 *  Needs the database, like SmartGazetteApplicationTests. */
@SpringBootTest(properties = {"admin.enabled=false", "scraper.enabled=false"})
@AutoConfigureMockMvc
class AdminSwitchTest {

    @Autowired
    private MockMvc mvc;

    @Test
    void adminPagesAreHidden() throws Exception {
        mvc.perform(get("/admin")).andExpect(status().isNotFound());
        mvc.perform(get("/admin/content")).andExpect(status().isNotFound());
        mvc.perform(get("/admin/run-scraper")).andExpect(status().isNotFound());
        mvc.perform(get("/delete/1")).andExpect(status().isNotFound());
        mvc.perform(post("/add")).andExpect(status().isNotFound());
        mvc.perform(get("/login")).andExpect(status().isNotFound());
    }

    @Test
    void publicPagesStayOpen() throws Exception {
        mvc.perform(get("/about")).andExpect(status().isOk());
        mvc.perform(get("/categories")).andExpect(status().isOk());
    }
}
