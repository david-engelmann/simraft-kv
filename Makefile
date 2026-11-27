.PHONY: ci fmt lint test doc bench deny python-test clean

ci: fmt lint doc deny test bench python-test

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

clean:
	cargo clean
