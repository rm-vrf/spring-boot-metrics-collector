package cn.batchfile.metrics.collector.api;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import cn.batchfile.metrics.collector.config.BeatConfig;

@RestController
public class BeatController {
	
	@Autowired
	private BeatConfig beatConfig;

	@GetMapping("/v1/beat/period")
	public int getPeriod() {
		return beatConfig.getPeriod();
	}

	@GetMapping("/v1/beat/hosts")
	public List<String> getHost() {
		return beatConfig.getHosts();
	}
	
	@GetMapping("/v1/beat/excludes")
	public List<String> getExcludes() {
		return beatConfig.getExcludes();
	}
	
	@PutMapping("/v1/beat/hosts")
	public List<String> putHosts(@RequestBody String[] hosts) {
		if (beatConfig.getHosts() == null) {
			beatConfig.setHosts(new ArrayList<>());
		}
		
		beatConfig.getHosts().clear();
		return patchHosts(hosts);
	}

	@PatchMapping("/v1/beat/hosts")
	public List<String> patchHosts(@RequestBody String[] hosts) {
		if (beatConfig.getHosts() == null) {
			beatConfig.setHosts(new ArrayList<>());
		}
		
		for (String host : hosts) {
			if (!beatConfig.getHosts().contains(host)) {
				beatConfig.getHosts().add(host);
			}
		}
		
		return beatConfig.getHosts();
	}
	
	@PutMapping("/v1/beat/excludes")
	public List<String> putExcludes(@RequestBody String[] excludes) {
		if (beatConfig.getExcludes() == null) {
			beatConfig.setExcludes(new ArrayList<>());
		}
		
		beatConfig.getExcludes().clear();
		return patchExcludes(excludes);
	}

	@PatchMapping("/v1/beat/excludes")
	public List<String> patchExcludes(@RequestBody String[] excludes) {
		if (beatConfig.getExcludes() == null) {
			beatConfig.setExcludes(new ArrayList<>());
		}
		
		for (String exclude : excludes) {
			if (!beatConfig.getExcludes().contains(exclude)) {
				beatConfig.getExcludes().add(exclude);
			}
		}
		
		return beatConfig.getExcludes();
	}
	
	
}
