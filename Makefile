.PHONY: ci fmt lint test doc bench deny python-test examples fuzz-check book clean

ci: fmt lint doc deny fuzz-check test bench python-test examples

fmt:
	cargo fmt --all -- --check

lint:
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked

test:
	cargo test --workspace --all-features --locked
	cargo run -p simraft-core --example embed --locked

bench:
	cargo bench --workspace --locked -- --test

deny:
	cargo deny check

python-test:
	cargo build --locked -p simraft-sim --bin simraft-sim
	cargo build --locked -p simraft-node --bin simraft-node
	python3 -m unittest discover -s python/tests -v

examples:
	cargo build --locked -p simraft-sim --bin simraft-sim
	cargo build --locked -p simraft-node --bin simraft-node
	python3 examples/chaos_sweep_and_replay.py
	sh examples/cluster_quickstart.sh

fuzz-check:
	CARGO_TARGET_DIR="$(CURDIR)/target" cargo fmt --manifest-path fuzz/Cargo.toml --all -- --check
	CARGO_TARGET_DIR="$(CURDIR)/target" cargo check --manifest-path fuzz/Cargo.toml --locked --bins

book:
	bash book/sync-docs.sh
	mdbook build book

clean:
	cargo clean
	rm -rf book/build book/src/docs
