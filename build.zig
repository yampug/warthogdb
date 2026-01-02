const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const mod = b.addModule("warthogdb", .{
        .root_source_file = b.path("src/root.zig"),

        .target = target,
    });

    const exe = b.addExecutable(.{
        .name = "warthogdb",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "warthogdb", .module = mod },
            },
        }),
    });

    b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");

    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);

    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });
    const run_mod_tests = b.addRunArtifact(mod_tests);
    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });

    const run_exe_tests = b.addRunArtifact(exe_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);

    const verify_exe = b.addExecutable(.{
        .name = "verify-compat",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/verify_compat.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    verify_exe.root_module.addImport("warthogdb", mod);

    const run_verify = b.addRunArtifact(verify_exe);
    const verify_step = b.step("verify-compat", "Run cross-verification (Java -> Zig)");
    verify_step.dependOn(&run_verify.step);

    const generate_exe = b.addExecutable(.{
        .name = "generate-compat",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/generate_compat.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    generate_exe.root_module.addImport("warthogdb", mod);

    const run_generate = b.addRunArtifact(generate_exe);
    const generate_step = b.step("generate-compat", "Generate data for reverse verification (Zig -> Java)");
    generate_step.dependOn(&run_generate.step);

    const binary_gen = b.addExecutable(.{
        .name = "binary-gen",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/binary_gen.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    binary_gen.root_module.addImport("warthogdb", mod);

    const run_binary_gen = b.addRunArtifact(binary_gen);
    const binary_gen_step = b.step("binary-gen", "Run binary generation for comparison");
    binary_gen_step.dependOn(&run_binary_gen.step);

    const benchmark_exe = b.addExecutable(.{
        .name = "benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/benchmark.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    benchmark_exe.root_module.addImport("warthogdb", mod);

    const run_benchmark = b.addRunArtifact(benchmark_exe);
    const benchmark_step = b.step("benchmark-native", "Run native Zig benchmark");
    benchmark_step.dependOn(&run_benchmark.step);

    const benchmark_threaded_exe = b.addExecutable(.{
        .name = "benchmark-threaded",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/benchmark_threaded.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    benchmark_threaded_exe.root_module.addImport("warthogdb", mod);

    const run_benchmark_threaded = b.addRunArtifact(benchmark_threaded_exe);
    const benchmark_threaded_step = b.step("benchmark-threaded", "Run threaded Zig benchmark");
    benchmark_threaded_step.dependOn(&run_benchmark_threaded.step);

    const lib = b.addLibrary(.{
        .name = "warthogdb",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/c_api.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .linkage = .dynamic,
    });
    lib.root_module.addImport("warthogdb", mod);
    b.installArtifact(lib);
}
