#!/usr/bin/env python3
# Assignment 5 – Open3D pipeline (Deer model)

import open3d as o3d
import numpy as np

# ================= CONFIG =================
MODEL_PATH = "deer.obj"  # your unique 3D model
NUM_POINTS = 50000
VOXEL_SIZE = 0.1


# ================= HELPERS =================
def print_mesh_info(mesh, title="Mesh", intersections=False):
    verts = np.asarray(mesh.vertices)
    tris = np.asarray(mesh.triangles) if len(mesh.triangles) > 0 else np.empty((0, 3))
    print(f"\n[{title}]")
    print(f"Vertices (points): {len(verts)}")
    print(f"Triangles: {len(tris)}")
    print(f"Has vertex colors: {mesh.has_vertex_colors()}")
    print(f"Has vertex normals: {mesh.has_vertex_normals()}")
    print(f"Intersections (with plane): {intersections}")


def print_pcd_info(pcd, title="Point Cloud", intersections=False):
    pts = np.asarray(pcd.points)
    print(f"\n[{title}]")
    print(f"Points: {len(pts)}")
    print(f"Has colors: {pcd.has_colors()}")
    print(f"Has normals: {pcd.has_normals()}")
    print(f"Intersections (with plane): {intersections}")


def print_voxel_info(vox, title="Voxel Grid", intersections=False):
    voxels = vox.get_voxels()
    has_color = any(v.color is not None for v in voxels) if voxels else False
    print(f"\n[{title}]")
    print(f"Voxels: {len(voxels)}")
    print(f"Has colors: {has_color}")
    print(f"Intersections (with plane): {intersections}")


# ========== STEP 1: LOAD & VISUALIZE ==========
print("\n[STEP 1] Loading and visualization of the original Deer model.")
print("Understanding: Load the triangle mesh and inspect geometry + attributes.")

mesh = o3d.io.read_triangle_mesh(MODEL_PATH)
if mesh.is_empty():
    raise RuntimeError(
        f"Failed to load mesh from {MODEL_PATH}. "
        "Check the file name, folder, and that it's triangulated (.ply/.obj/.stl)."
    )

if not mesh.has_vertex_normals():
    mesh.compute_vertex_normals()
mesh.paint_uniform_color([0.7, 0.7, 0.7])

print_mesh_info(mesh, title="1. Original Deer Mesh")
o3d.visualization.draw_geometries([mesh], window_name="1. Original Deer Mesh")

# ========== STEP 2: CONVERSION TO POINT CLOUD ==========
print("\n--- Task 2: Conversion to Point Cloud ---")
try:
    # Robust conversion for OBJ/STL models
    point_cloud = mesh.sample_points_poisson_disk(
        number_of_points=NUM_POINTS, init_factor=5
    )

    # Colorize by X axis for clarity
    pts = np.asarray(point_cloud.points)
    if pts.size:
        x = pts[:, 0]
        x_norm = (x - x.min()) / (x.max() - x.min() + 1e-9)
        point_cloud.colors = o3d.utility.Vector3dVector(
            np.stack([x_norm, 0.2 * np.ones_like(x_norm), 1.0 - x_norm], axis=1)
        )

    o3d.visualization.draw_geometries([point_cloud], window_name="2. Deer Point Cloud")
    print(f"Number of vertices (points): {len(point_cloud.points)}")
    print(f"Has colors: {point_cloud.has_colors()}")

    pcd = point_cloud
except Exception as e:
    print(f"Error in point cloud conversion: {e}")
    raise

# ========== STEP 3: SURFACE RECONSTRUCTION (POISSON) ==========
print("\n[STEP 3] Surface reconstruction from point cloud (Poisson).")
print("Understanding: Rebuild a continuous surface from scattered points.")

pcd.estimate_normals()
pcd.orient_normals_consistent_tangent_plane(30)

mesh_poisson, densities = o3d.geometry.TriangleMesh.create_from_point_cloud_poisson(
    pcd, depth=9
)

# --- Clean up mesh by density filtering and cropping ---
dens = np.asarray(densities)
keep = dens > np.quantile(dens, 0.05)
mesh_poisson = mesh_poisson.select_by_index(np.where(keep)[0])
mesh_poisson.remove_unreferenced_vertices()

bbox = mesh.get_axis_aligned_bounding_box().scale(1.05, mesh.get_center())
mesh_poisson = mesh_poisson.crop(bbox)

# topology cleanup & smoothing
mesh_poisson.remove_degenerate_triangles()
mesh_poisson.remove_duplicated_triangles()
mesh_poisson.remove_duplicated_vertices()
mesh_poisson.remove_non_manifold_edges()
mesh_poisson = mesh_poisson.filter_smooth_taubin(number_of_iterations=5)
mesh_poisson.compute_vertex_normals()

print_mesh_info(mesh_poisson, title="3. Reconstructed Deer Mesh (Poisson)")
o3d.visualization.draw_geometries(
    [mesh_poisson], window_name="3. Reconstructed Deer Mesh (Poisson)"
)

# keep clean copy for Step 7
mesh_for_step7 = (
    mesh_poisson.clone()
    if hasattr(mesh_poisson, "clone")
    else o3d.geometry.TriangleMesh(mesh_poisson)
)

# ========== STEP 4: VOXELIZATION ==========
print("\n--- Task 4: Voxelization ---")
try:
    # Adjust voxel size based on your model scale
    voxel_size = 0.99  # Reasonable size for detail
    voxel_grid = o3d.geometry.VoxelGrid.create_from_point_cloud(
        point_cloud, voxel_size=voxel_size
    )

    # Display voxel grid
    o3d.visualization.draw_geometries([voxel_grid], window_name="Task 4: Voxel Grid")

    # Print required information
    print(f"Voxel size: {voxel_size}")
    print(f"Number of voxels: {len(voxel_grid.get_voxels())}")

except Exception as e:
    print(f"Error in voxelization: {e}")

# ========== STEP 5: ADD A PLANE ==========
print("\n[STEP 5] Adding a plane for reference/clipping.")
bbox = mesh.get_axis_aligned_bounding_box()
center = bbox.get_center()
extent = bbox.get_extent()

plane = o3d.geometry.TriangleMesh.create_box(
    width=0.01, height=extent[1] * 1.2, depth=extent[2] * 1.2
)
plane.translate(
    [center[0] - 0.005, center[1] - extent[1] * 0.6, center[2] - extent[2] * 0.6]
)
plane.paint_uniform_color([0.2, 0.2, 0.2])

o3d.visualization.draw_geometries([mesh, plane], window_name="5. Plane + Deer")

# ========== STEP 6: CLIP BY PLANE ==========
print("\n[STEP 6] Clipping the mesh by the plane.")
plane_point = np.array([center[0], 0.0, 0.0])
plane_normal = np.array([1.0, 0.0, 0.0])

verts = np.asarray(mesh.vertices)
dots = np.dot(verts - plane_point, plane_normal)
keep_mask = dots <= 0.0
intersects = (dots.min() < 0.0) and (dots.max() > 0.0)

old_to_new = -np.ones(len(verts), dtype=int)
new_vertices = []
for i, keep in enumerate(keep_mask):
    if keep:
        old_to_new[i] = len(new_vertices)
        new_vertices.append(verts[i])

new_vertices = np.asarray(new_vertices)
triangles = np.asarray(mesh.triangles)
new_triangles = []
for tri in triangles:
    i0, i1, i2 = old_to_new[tri]
    if i0 != -1 and i1 != -1 and i2 != -1:
        new_triangles.append([i0, i1, i2])

clipped_mesh = o3d.geometry.TriangleMesh()
clipped_mesh.vertices = o3d.utility.Vector3dVector(new_vertices)
clipped_mesh.triangles = o3d.utility.Vector3iVector(
    np.asarray(new_triangles, dtype=np.int32)
)
clipped_mesh.compute_vertex_normals()
clipped_mesh.paint_uniform_color([0.9, 0.9, 0.9])

print_mesh_info(clipped_mesh, title="6. Clipped Deer Mesh", intersections=intersects)
o3d.visualization.draw_geometries([clipped_mesh], window_name="6. Clipped Deer Mesh")

# ========== STEP 7: COLOR GRADIENT & EXTREMES ==========
print("\n[STEP 7] Color gradient and extremes (full mesh).")
base = mesh_for_step7
if base.is_empty() or len(base.vertices) == 0:
    print("Selected base mesh for step 7 is empty.")
else:
    verts = np.asarray(base.vertices)
    axis_vals = verts[:, 2]  # Z-axis
    vmin, vmax = axis_vals.min(), axis_vals.max()
    norm = (axis_vals - vmin) / (vmax - vmin + 1e-9)
    colors = np.stack([norm, 0.0 * norm, 1.0 - norm], axis=1)
    base.vertex_colors = o3d.utility.Vector3dVector(colors)

    min_idx = int(np.argmin(axis_vals))
    max_idx = int(np.argmax(axis_vals))
    p_min = verts[min_idx]
    p_max = verts[max_idx]
    print(f"Extremes along Z-axis:\n  Min Z: {p_min}\n  Max Z: {p_max}")

    scale = np.linalg.norm(base.get_max_bound() - base.get_min_bound())
    r = 0.02 * scale
    s_min = o3d.geometry.TriangleMesh.create_sphere(radius=r).translate(p_min)
    s_max = o3d.geometry.TriangleMesh.create_sphere(radius=r).translate(p_max)
    s_min.paint_uniform_color([1, 1, 0])
    s_max.paint_uniform_color([0, 1, 0])

    o3d.visualization.draw_geometries(
        [base, s_min, s_max], window_name="7. Gradient Color + Z-Extremes (FULL Deer)"
    )

print("\nAll 7 steps completed successfully.")
