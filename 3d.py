import open3d as o3d
import numpy as np
import copy


def assignment_5_3d_processing():
    print("=== Assignment #5: 3D Processing with Open3D ===\n")

    # Task 1: Loading and Visualization
    print("--- Task 1: Loading and Visualization ---")
    try:
        # Load your deer model
        mesh = o3d.io.read_triangle_mesh("deer.obj")  # Adjust filename

        # Compute normals if missing
        if not mesh.has_vertex_normals():
            mesh.compute_vertex_normals()

        # Display original model
        o3d.visualization.draw_geometries([mesh], window_name="Task 1: Original Model")

        # Print required information
        print(f"Number of vertices: {len(mesh.vertices)}")
        print(f"Number of triangles: {len(mesh.triangles)}")
        print(f"Has vertex colors: {mesh.has_vertex_colors()}")
        print(f"Has vertex normals: {mesh.has_vertex_normals()}")

    except Exception as e:
        print(f"Error loading model: {e}")
        return

    # Task 2: Conversion to Point Cloud
    print("\n--- Task 2: Conversion to Point Cloud ---")
    try:
        # Convert to point cloud - use more points for better reconstruction
        point_cloud = mesh.sample_points_poisson_disk(number_of_points=10000)

        # Compute normals for surface reconstruction
        point_cloud.estimate_normals(
            search_param=o3d.geometry.KDTreeSearchParamHybrid(radius=0.1, max_nn=30)
        )

        # Display point cloud
        o3d.visualization.draw_geometries(
            [point_cloud], window_name="Task 2: Point Cloud"
        )

        # Print required information
        print(f"Number of vertices (points): {len(point_cloud.points)}")
        print(f"Has colors: {point_cloud.has_colors()}")
        print(f"Has normals: {point_cloud.has_normals()}")

    except Exception as e:
        print(f"Error in point cloud conversion: {e}")
        return

    # Task 3: Surface Reconstruction from Point Cloud
    print("\n--- Task 3: Surface Reconstruction ---")
    try:
        # Ensure point cloud has normals
        if not point_cloud.has_normals():
            print("Estimating normals...")
            point_cloud.estimate_normals()

        print("Running Poisson surface reconstruction...")

        mesh_reconstructed, densities = (
            o3d.geometry.TriangleMesh.create_from_point_cloud_poisson(
                point_cloud,
                depth=7,
                width=0,
                scale=1.1,
                linear_fit=False,
            )
        )

        print("Cropping with bounding box...")
        # Simple bounding box cropping
        bbox = point_cloud.get_axis_aligned_bounding_box()
        mesh_reconstructed = mesh_reconstructed.crop(bbox)

        # Compute normals for visualization
        mesh_reconstructed.compute_vertex_normals()

        # Display reconstructed mesh
        o3d.visualization.draw_geometries(
            [mesh_reconstructed], window_name="Task 3: Reconstructed Mesh"
        )

        # Print required information
        print(f"Number of vertices: {len(mesh_reconstructed.vertices)}")
        print(f"Number of triangles: {len(mesh_reconstructed.triangles)}")
        print(f"Has vertex colors: {mesh_reconstructed.has_vertex_colors()}")
        print("✓ Surface reconstruction completed successfully!")

    except Exception as e:
        print(f"❌ Poisson reconstruction failed: {e}")
        print("Trying convex hull as fallback...")
        try:
            hull, _ = point_cloud.compute_convex_hull()
            hull.compute_vertex_normals()
            o3d.visualization.draw_geometries([hull], window_name="Task 3: Convex Hull")
            print(f"Number of vertices: {len(hull.vertices)}")
            print(f"Number of triangles: {len(hull.triangles)}")
            mesh_reconstructed = hull
            print("✓ Convex hull created as fallback")
        except Exception as e2:
            print(f"❌ All reconstruction methods failed: {e2}")
            return

    # Task 4: Voxelization
    print("\n--- Task 4: Voxelization ---")
    try:
        # Adjust voxel size based on your model scale
        voxel_size = 0.99  # Reasonable size for detail
        voxel_grid = o3d.geometry.VoxelGrid.create_from_point_cloud(
            point_cloud, voxel_size=voxel_size
        )

        # Display voxel grid
        o3d.visualization.draw_geometries(
            [voxel_grid], window_name="Task 4: Voxel Grid"
        )

        # Print required information
        print(f"Voxel size: {voxel_size}")
        print(f"Number of voxels: {len(voxel_grid.get_voxels())}")

    except Exception as e:
        print(f"Error in voxelization: {e}")
        return

    # ========== TASK 5: ADDING A PLANE ==========
    print("\n--- Task 5: Adding a Plane ---")
    try:
        # Your specific plane implementation
        bbox = mesh.get_axis_aligned_bounding_box()
        center = bbox.get_center()
        extent = bbox.get_extent()

        # Create a vertical plane (aligned with YZ plane)
        plane = o3d.geometry.TriangleMesh.create_box(
            width=0.01, height=extent[1] * 1.2, depth=extent[2] * 1.2
        )

        # Position the plane to cut through the deer
        plane.translate(
            [
                center[0] - 0.005,
                center[1] - extent[1] * 0.6,
                center[2] - extent[2] * 0.6,
            ]
        )
        plane.paint_uniform_color([0.2, 0.2, 0.2])  # Dark gray
        plane.compute_vertex_normals()

        # Display mesh and plane together
        o3d.visualization.draw_geometries(
            [mesh, plane], window_name="Task 5: Deer with Cutting Plane"
        )

        print("✓ Plane created and positioned for clipping")
        print(f"Plane position: center around X = {center[0]:.3f}")
        print(f"Plane dimensions: {extent[1]*1.2:.3f} x {extent[2]*1.2:.3f}")

    except Exception as e:
        print(f"Error creating plane: {e}")
        return

    # Task 6: Surface Clipping
    print("\n--- Task 6: Surface Clipping ---")
    try:
        # Simple point cloud clipping (more reliable than mesh clipping)
        vertices = np.asarray(point_cloud.points)

        # Get bounding box to determine clipping plane
        bbox = point_cloud.get_axis_aligned_bounding_box()
        center = bbox.get_center()

        # Clip points on the right side (x > center_x)
        clipping_threshold = center[0]
        left_mask = vertices[:, 0] <= clipping_threshold
        left_points = vertices[left_mask]

        # Create clipped point cloud
        pc_clipped = o3d.geometry.PointCloud()
        pc_clipped.points = o3d.utility.Vector3dVector(left_points)

        # Transfer colors if available
        if point_cloud.has_colors():
            colors = np.asarray(point_cloud.colors)
            pc_clipped.colors = o3d.utility.Vector3dVector(colors[left_mask])

        # Display clipped geometry
        o3d.visualization.draw_geometries(
            [pc_clipped], window_name="Task 6: Clipped Point Cloud"
        )

        # Print required information
        print(f"Number of remaining vertices: {len(pc_clipped.points)}")
        print(f"Original vertices: {len(point_cloud.points)}")
        print(f"Vertices removed: {len(point_cloud.points) - len(pc_clipped.points)}")
        print(f"Has colors: {pc_clipped.has_colors()}")
        print(f"Has normals: {pc_clipped.has_normals()}")

    except Exception as e:
        print(f"Error in surface clipping: {e}")
        return

    # ========== TASK 7: WORKING WITH COLOR AND EXTREMES (FIXED) ==========
    print("\n--- Task 7: Color Gradient and Extremes ---")
    try:
        # Use the original mesh for coloring
        mesh_colored = copy.deepcopy(mesh)

        # 1) Remove original colors and apply Z-axis gradient
        vertices = np.asarray(mesh_colored.vertices)
        z_coords = vertices[:, 2]
        z_min, z_max = np.min(z_coords), np.max(z_coords)
        z_range = z_max - z_min

        # Create color gradient based on Z-axis (blue to red)
        colors = np.zeros((len(vertices), 3))
        for i, z in enumerate(z_coords):
            normalized_z = (z - z_min) / z_range if z_range > 0 else 0.5
            colors[i] = [normalized_z, 0.3, 1 - normalized_z]  # Blue to Red

        mesh_colored.vertex_colors = o3d.utility.Vector3dVector(colors)
        mesh_colored.compute_vertex_normals()

        # 2) Find extreme points along Z-axis
        min_idx = np.argmin(z_coords)
        max_idx = np.argmax(z_coords)
        min_point = vertices[min_idx]
        max_point = vertices[max_idx]

        print(f"Minimum point found at: {min_point}")
        print(f"Maximum point found at: {max_point}")

        # 3) Create LARGE VISIBLE markers for extremes
        # Calculate model scale for proper marker sizing
        bbox = mesh.get_axis_aligned_bounding_box()
        extent = bbox.get_extent()
        model_scale = np.max(extent)

        # Use LARGE markers (20% of model size)
        marker_size = model_scale * 0.2
        if marker_size < 0.1:  # Minimum size
            marker_size = 0.1
        if marker_size > 0.5:  # Maximum size
            marker_size = 0.3

        print(f"Using marker size: {marker_size:.3f}")

        # OPTION A: LARGE COLORED SPHERES (most visible)
        min_sphere = o3d.geometry.TriangleMesh.create_sphere(radius=marker_size)
        min_sphere.translate(min_point)
        min_sphere.paint_uniform_color([0.0, 1.0, 0.0])  # Bright green
        min_sphere.compute_vertex_normals()

        max_sphere = o3d.geometry.TriangleMesh.create_sphere(radius=marker_size)
        max_sphere.translate(max_point)
        max_sphere.paint_uniform_color([1.0, 0.0, 0.0])  # Bright red
        max_sphere.compute_vertex_normals()

        # OPTION B: WIREFRAME CUBES (alternative visibility)
        min_cube = o3d.geometry.TriangleMesh.create_box(
            width=marker_size, height=marker_size, depth=marker_size
        )
        min_cube.translate(
            min_point - [marker_size / 2, marker_size / 2, marker_size / 2]
        )
        min_cube.paint_uniform_color([0.0, 1.0, 0.0])  # Green
        min_cube.compute_vertex_normals()

        max_cube = o3d.geometry.TriangleMesh.create_box(
            width=marker_size, height=marker_size, depth=marker_size
        )
        max_cube.translate(
            max_point - [marker_size / 2, marker_size / 2, marker_size / 2]
        )
        max_cube.paint_uniform_color([1.0, 0.0, 0.0])  # Red
        max_cube.compute_vertex_normals()

        # OPTION C: COORDINATE AXES at extreme points (very clear)
        min_axes = o3d.geometry.TriangleMesh.create_coordinate_frame(size=marker_size)
        min_axes.translate(min_point)

        max_axes = o3d.geometry.TriangleMesh.create_coordinate_frame(size=marker_size)
        max_axes.translate(max_point)

        # 4) Create main coordinate frame
        main_axes = o3d.geometry.TriangleMesh.create_coordinate_frame(
            size=marker_size * 1.5
        )

        # 5) Display with MULTIPLE HIGHLIGHTING METHODS
        print("Displaying with LARGE visible markers...")

        # Show with spheres only first
        o3d.visualization.draw_geometries(
            [mesh_colored, min_sphere, max_sphere, main_axes],
            window_name="Task 7: Extremes with LARGE Spheres",
        )

        # Show with cubes only
        o3d.visualization.draw_geometries(
            [mesh_colored, min_cube, max_cube, main_axes],
            window_name="Task 7: Extremes with LARGE Cubes",
        )

        # Show with coordinate axes at extremes
        o3d.visualization.draw_geometries(
            [mesh_colored, min_axes, max_axes, main_axes],
            window_name="Task 7: Extremes with Coordinate Axes",
        )

        # Show ALL markers together (maximum visibility)
        o3d.visualization.draw_geometries(
            [
                mesh_colored,
                min_sphere,
                max_sphere,
                min_cube,
                max_cube,
                min_axes,
                max_axes,
                main_axes,
            ],
            window_name="Task 7: ALL Highlighting Methods",
        )

        # 6) Print required information
        print(
            f"Z-axis minimum point: ({min_point[0]:.3f}, {min_point[1]:.3f}, {min_point[2]:.3f})"
        )
        print(
            f"Z-axis maximum point: ({max_point[0]:.3f}, {max_point[1]:.3f}, {max_point[2]:.3f})"
        )
        print(f"Z-axis range: {z_range:.3f}")
        print(f"Model scale: {model_scale:.3f}")
        print(f"Marker size used: {marker_size:.3f}")
        print("✓ Color gradient and extremes with PROPER highlighting completed")

    except Exception as e:
        print(f"Error in color and extremes: {e}")
        import traceback

        traceback.print_exc()
        return

    print("\n🎉 === All 7 tasks completed successfully! === 🎉")


if __name__ == "__main__":
    assignment_5_3d_processing()
