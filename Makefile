.PHONY: docker_images
docker_images:
	docker build --target scout_control -t scout/control .
	docker build --target scout_data -t scout/data .
	docker build --target scout_api -t scout/api .
	docker build --target scout_admin -t scout/admin .
