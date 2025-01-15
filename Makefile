pycache := $(shell find $(abspath .) -iname __pycache__)

sparse_src_dir  := $(abspath ./sparse_framework)
sparse_py       := $(shell find $(sparse_src_dir) -iname '*.py')
sparse_stats_dir := /var/lib/sparse/stats

docker_image      := anterondocker/sparse-framework
dockerfile        := Dockerfile
docker_build_file := .DOCKER
docker_build_file_pylint := .DOCKER_PYLINT

docker_tag_graphs        := graphs
docker_tag_pylint        := pylint
dockerfile_graphs        := Dockerfile.graphs
dockerfile_pylint        := Dockerfile.pylint
docker_build_file_graphs := .DOCKER_GRAPHS
py_requirements_graphs   := requirements_graphs.txt
py_requirements_pylint   := requirements_pylint.txt

ifneq (,$(shell uname -a | grep tegra))
	docker_base_image=nvcr.io/nvidia/l4t-pytorch:r34.1.0-pth1.12-py3
	docker_tag=l4t-pytorch
	docker_py_requirements=requirements_jetson.txt
else
	docker_base_image=pytorch/pytorch:2.2.2-cuda12.1-cudnn8-runtime
	docker_tag=pytorch
	docker_py_requirements=requirements.txt
endif

$(docker_build_file): $(sparse_py) $(dockerfile)
	docker build . --no-cache \
                 --build-arg BASE_IMAGE=$(docker_base_image) \
								 --build-arg PY_REQUIREMENTS=$(docker_py_requirements) \
								 -t $(docker_image):$(docker_tag)
	docker build . -f $(dockerfile_graphs) \
		-t $(docker_image):$(docker_tag_graphs)
	docker image prune -f
	touch $(docker_build_file)

$(docker_build_file_pylint): $(dockerfile_pylint) $(py_requirements_pylint)
	docker build . -f $(dockerfile_pylint) \
		-t $(docker_image):$(docker_tag_pylint)
	touch $(docker_build_file_pylint)

.PHONY: docker clean run run-pylint run-tests run-experiment clean-experiment graphs

docker: $(docker_build_file)
	#make -C examples/splitnn docker
	# Deprecated
#	make -C examples/deprune docker

clean:
	scripts/delete_worker.sh

run:
	scripts/deploy_worker.sh

run-pylint: $(docker_build_file_pylint)
	docker run --rm -v $(abspath .):/app $(docker_image):$(docker_tag_pylint)

run-tests: $(docker_build_file_pylint)
	docker run --rm -v $(abspath .):/app $(docker_image):$(docker_tag_pylint) python -m unittest -v

run-experiment:
	scripts/run-experiment.sh

clean-experiment:
	scripts/clean-experiment.sh

graphs:
	docker run --rm -v $(sparse_stats_dir):$(sparse_stats_dir) \
		              -v $(abspath .):/app \
									-it $(docker_image):$(docker_tag_graphs)
