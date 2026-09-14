from src.workload import WorkloadSpec, generate_workload


def test_reproducibility_fixed_seed():
    spec = WorkloadSpec("mixed", n_messages=25)
    first, _ = generate_workload(spec, 42, "a")
    second, _ = generate_workload(spec, 42, "a")
    assert first == second

