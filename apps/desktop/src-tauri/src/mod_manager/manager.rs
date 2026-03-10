use crate::errors::Error;
use crate::mod_manager::{
  addons_backup_manager::AddonsBackupManager,
  autoexec_manager::AutoexecManager,
  file_tree::{FileTreeAnalyzer, ModFileTree},
  filesystem_helper::FileSystemHelper,
  game_config_manager::GameConfigManager,
  game_process_manager::GameProcessManager,
  mod_repository::{Mod, ModRepository},
  steam_manager::SteamManager,
  vpk_manager::VpkManager,
};
use log;
use std::path::PathBuf;
use tauri::Manager;

pub struct ModManager {
  steam_manager: SteamManager,
  process_manager: GameProcessManager,
  config_manager: GameConfigManager,
  vpk_manager: VpkManager,
  file_tree_analyzer: FileTreeAnalyzer,
  filesystem: FileSystemHelper,
  mod_repository: ModRepository,
  addons_backup_manager: AddonsBackupManager,
  autoexec_manager: AutoexecManager,
  app_handle: Option<tauri::AppHandle>,
}

impl ModManager {
  pub fn new() -> Self {
    let mut manager = Self {
      steam_manager: SteamManager::new(),
      process_manager: GameProcessManager::new(),
      config_manager: GameConfigManager::new(),
      vpk_manager: VpkManager::new(),
      file_tree_analyzer: FileTreeAnalyzer::new(),
      filesystem: FileSystemHelper::new(),
      mod_repository: ModRepository::new(),
      addons_backup_manager: AddonsBackupManager::new(),
      autoexec_manager: AutoexecManager::new(),
      app_handle: None,
    };

    // Try to find the game path on initialization
    if let Err(e) = manager.find_game() {
      log::warn!("Failed to find game path during initialization: {e:?}");
    }

    manager
  }

  pub fn find_steam(&mut self) -> Result<(), Error> {
    self.steam_manager.find_steam()?;
    Ok(())
  }

  pub fn find_game(&mut self) -> Result<PathBuf, Error> {
    let game_path = self.steam_manager.find_game()?.clone();
    Ok(game_path)
  }

  pub fn set_game_path(&mut self, path: PathBuf) -> Result<PathBuf, Error> {
    self.steam_manager.set_game_path(path.clone())?;
    self.addons_backup_manager.set_game_path(path.clone());
    Ok(path)
  }

  pub fn is_game_running(&mut self) -> Result<bool, Error> {
    self.process_manager.is_game_running()
  }

  pub fn stop_game(&mut self) -> Result<(), Error> {
    self.process_manager.stop_game()
  }

  pub fn setup_game_for_mods(&mut self) -> Result<(), Error> {
    // Ensure game is not running before setup
    self.process_manager.ensure_game_not_running()?;

    if self.config_manager.is_game_setup() {
      log::info!("Game already setup");
      return Ok(());
    }

    let game_path = self.steam_manager.find_game()?;
    self.config_manager.validate_game_files(game_path)?;
    self.config_manager.setup_game_for_mods(game_path)?;

    Ok(())
  }

  pub fn toggle_mods(&mut self, vanilla: bool) -> Result<(), Error> {
    let game_path = self
      .steam_manager
      .get_game_path()
      .ok_or(Error::GamePathNotSet)?;

    self.config_manager.toggle_mods(game_path, vanilla)?;

    Ok(())
  }

  pub fn run_game(
    &mut self,
    vanilla: bool,
    additional_args: String,
    profile_folder: Option<String>,
  ) -> Result<(), Error> {
    // Ensure game path is found
    let game_path = self.find_game()?;

    // Toggle mods based on vanilla flag
    if vanilla {
      log::info!("Disabling mods...");
      self.toggle_mods(vanilla)?;
    } else {
      log::info!("Enabling mods for profile: {:?}...", profile_folder);
      // Use update_mod_path to set the correct profile folder path
      self
        .config_manager
        .update_mod_path(&game_path, profile_folder)?;
    }

    // Launch the game through Steam
    self.steam_manager.launch_game(&additional_args)?;

    Ok(())
  }

  pub fn get_mod_file_tree(&self, mod_path: &PathBuf) -> Result<ModFileTree, Error> {
    self.file_tree_analyzer.get_mod_file_tree(mod_path)
  }

  /// Get the store files directory for a mod: mods/{modId}/files/
  pub fn get_mod_files_dir(&self, mod_id: &str) -> Result<PathBuf, Error> {
    Ok(self.get_mods_store_path()?.join(mod_id).join("files"))
  }

  /// Get the addons path for a profile
  fn get_addons_path(&self, profile_folder: Option<&String>) -> Result<PathBuf, Error> {
    let game_path = self
      .steam_manager
      .get_game_path()
      .ok_or(Error::GamePathNotSet)?;

    Ok(if let Some(folder) = profile_folder {
      game_path
        .join("game")
        .join("citadel")
        .join("addons")
        .join(folder)
    } else {
      game_path.join("game").join("citadel").join("addons")
    })
  }

  pub fn install_mod(
    &mut self,
    mut deadlock_mod: Mod,
    profile_folder: Option<String>,
  ) -> Result<Mod, Error> {
    log::info!(
      "Starting installation (enable) of mod: {} (profile: {profile_folder:?})",
      deadlock_mod.name
    );

    if !self.config_manager.is_game_setup() {
      log::info!("Setting up game for mods...");
      self.setup_game_for_mods()?;
    }

    let addons_path = self.get_addons_path(profile_folder.as_ref())?;
    let store_files_dir = self.get_mod_files_dir(&deadlock_mod.id)?;

    // Primary path: link from mod store to addons
    if store_files_dir.exists()
      && !self
        .filesystem
        .get_files_with_extension(&store_files_dir, "vpk")?
        .is_empty()
    {
      log::info!(
        "Store files found at {:?}, linking to addons",
        store_files_dir
      );

      let original_names: Vec<String> = self
        .filesystem
        .get_files_with_extension(&store_files_dir, "vpk")?
        .iter()
        .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(String::from))
        .collect();

      let installed_vpks = self
        .vpk_manager
        .link_vpks_to_addons(&store_files_dir, &addons_path)?;

      deadlock_mod.installed_vpks = installed_vpks;
      deadlock_mod.original_vpk_names = original_names;
    } else {
      // Legacy fallback: migrate prefixed VPKs from addons into the store
      log::info!(
        "No store files for mod {}, trying legacy migration",
        deadlock_mod.id
      );
      return self.install_mod_legacy(deadlock_mod, profile_folder);
    }

    log::info!("Adding mod to managed mods list");
    self.mod_repository.add_mod(deadlock_mod.clone());

    // If the mod has an install order, trigger a reorder to maintain correct sequence
    if deadlock_mod.install_order.is_some() {
      log::info!("Mod has install order, triggering reorder to maintain sequence");
      self.reorder_all_mods_for_profile(profile_folder)?;
    }

    log::info!("Mod installation (enable) completed successfully");
    Ok(deadlock_mod)
  }

  /// Legacy install path: handles mods that still use prefixed VPKs in addons.
  /// Migrates them into the store before enabling via hardlinks.
  fn install_mod_legacy(
    &mut self,
    mut deadlock_mod: Mod,
    profile_folder: Option<String>,
  ) -> Result<Mod, Error> {
    let addons_path = self.get_addons_path(profile_folder.as_ref())?;
    let store_files_dir = self.get_mod_files_dir(&deadlock_mod.id)?;

    // Find prefixed VPKs in addons
    let prefixed_vpks = self
      .vpk_manager
      .find_prefixed_vpks(&addons_path, &deadlock_mod.id)?;

    if prefixed_vpks.is_empty() {
      log::error!(
        "No store files and no prefixed VPKs found for mod {}",
        deadlock_mod.id
      );
      return Err(Error::ModInvalid(
        "Mod needs to be downloaded first. No VPK files found.".into(),
      ));
    }

    log::info!(
      "Migrating {} prefixed VPKs to store for mod {}",
      prefixed_vpks.len(),
      deadlock_mod.id
    );

    // Migrate: copy prefixed VPKs from addons to store (stripping prefix)
    self.filesystem.create_directories(&store_files_dir)?;
    let prefix = format!("{}_", deadlock_mod.id);

    for prefixed_name in &prefixed_vpks {
      let original_name = prefixed_name
        .strip_prefix(&prefix)
        .unwrap_or(prefixed_name);
      let src = addons_path.join(prefixed_name);
      let dest = store_files_dir.join(original_name);
      self.filesystem.copy_file(&src, &dest)?;
      log::info!("Migrated {prefixed_name} -> store/{original_name}");
    }

    // Remove old prefixed VPKs from addons
    for prefixed_name in &prefixed_vpks {
      let path = addons_path.join(prefixed_name);
      self.filesystem.remove_file(&path)?;
    }

    // Now enable via hardlinks from the store
    let original_names: Vec<String> = self
      .filesystem
      .get_files_with_extension(&store_files_dir, "vpk")?
      .iter()
      .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(String::from))
      .collect();

    let installed_vpks = self
      .vpk_manager
      .link_vpks_to_addons(&store_files_dir, &addons_path)?;

    deadlock_mod.installed_vpks = installed_vpks;
    deadlock_mod.original_vpk_names = original_names;

    log::info!("Adding mod to managed mods list");
    self.mod_repository.add_mod(deadlock_mod.clone());

    if deadlock_mod.install_order.is_some() {
      log::info!("Mod has install order, triggering reorder to maintain sequence");
      self.reorder_all_mods_for_profile(profile_folder)?;
    }

    log::info!("Legacy mod installation (enable) completed successfully");
    Ok(deadlock_mod)
  }

  pub fn uninstall_mod(
    &mut self,
    mod_id: String,
    vpks: Vec<String>,
    profile_folder: Option<String>,
  ) -> Result<(), Error> {
    log::info!("Uninstalling (disabling) mod: {mod_id} (profile: {profile_folder:?})");

    let addons_path = self.get_addons_path(profile_folder.as_ref())?;

    if !addons_path.exists() {
      return Err(Error::GamePathNotSet);
    }

    // Determine which VPKs to remove from addons
    let installed_vpks = if let Some(local_mod) = self.mod_repository.get_mod(&mod_id) {
      log::info!("Mod found in memory: {}", local_mod.name);
      local_mod.installed_vpks.clone()
    } else if !vpks.is_empty() {
      log::warn!("Mod not found in repository, using VPKs from frontend");
      vpks
        .iter()
        .map(|v| {
          std::path::Path::new(v)
            .file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_else(|| v.clone())
        })
        .collect()
    } else {
      log::warn!("Mod not found in repository and no VPKs provided");
      return Ok(());
    };

    // Remove VPK links/copies from addons (originals safe in mod store)
    self
      .vpk_manager
      .unlink_vpks_from_addons(&addons_path, &installed_vpks)?;

    // Also clean up any legacy prefixed VPKs that may exist
    self
      .vpk_manager
      .remove_vpks_by_mod_id(&addons_path, &mod_id)?;

    // Update mod state
    if let Some(mut local_mod) = self.mod_repository.get_mod(&mod_id).cloned() {
      local_mod.installed_vpks = Vec::new();
      self.mod_repository.add_mod(local_mod);
    }

    log::info!("Disabled mod {mod_id}");
    Ok(())
  }

  pub fn purge_mod(
    &mut self,
    mod_id: String,
    vpks: Vec<String>,
    profile_folder: Option<String>,
  ) -> Result<(), Error> {
    log::info!("Purging mod: {mod_id} (profile: {profile_folder:?})");

    let addons_path = self.get_addons_path(profile_folder.as_ref())?;

    if !addons_path.exists() {
      return Err(Error::GamePathNotSet);
    }

    // Remove VPK files from addons (both installed links and legacy prefixed)
    if let Some(local_mod) = self.mod_repository.remove_mod(&mod_id) {
      log::info!("Mod found in memory: {}", local_mod.name);

      if !local_mod.installed_vpks.is_empty() {
        self
          .vpk_manager
          .unlink_vpks_from_addons(&addons_path, &local_mod.installed_vpks)?;
      }
    } else if !vpks.is_empty() {
      self.vpk_manager.remove_vpks(&vpks, &addons_path)?;
    }

    // Also remove any legacy prefixed VPKs
    self
      .vpk_manager
      .remove_vpks_by_mod_id(&addons_path, &mod_id)?;

    // Remove the mod's folder from user's local app data (including store files)
    let mods_path = self.get_mods_store_path()?;
    let user_mod_dir = mods_path.join(&mod_id);

    if user_mod_dir.exists() {
      log::info!("Removing user-mod folder: {user_mod_dir:?}");
      self.filesystem.remove_directory_recursive(&user_mod_dir)?;
    } else {
      log::warn!("User-mod folder not found, skipping: {user_mod_dir:?}");
    }

    Ok(())
  }

  /// Reorder all mods based on their current install_order for a specific profile
  fn reorder_all_mods_for_profile(&mut self, profile_folder: Option<String>) -> Result<(), Error> {
    let addons_path = self.get_addons_path(profile_folder.as_ref())?;
    let mods_store = self.get_mods_store_path()?;

    log::info!("Reordering all mods based on install order for profile: {profile_folder:?}");

    // Collect all enabled mods and sort by install order
    let mut ordered_mods: Vec<Mod> = self
      .mod_repository
      .get_all_mods()
      .filter(|m| !m.installed_vpks.is_empty())
      .cloned()
      .collect();
    ordered_mods.sort_by_key(|mod_entry| mod_entry.install_order.unwrap_or(999));

    // Build store-based mapping for reordering
    let mod_store_mapping: Vec<(String, PathBuf)> = ordered_mods
      .iter()
      .map(|m| (m.id.clone(), mods_store.join(&m.id).join("files")))
      .collect();

    let updated_vpk_mappings = self
      .vpk_manager
      .reorder_vpks_from_store(&mod_store_mapping, &addons_path)?;

    // Update mod data with new VPK names
    for (mod_id, new_vpk_names) in updated_vpk_mappings {
      if let Some(mut mod_entry) = self.mod_repository.remove_mod(&mod_id) {
        mod_entry.installed_vpks = new_vpk_names;
        self.mod_repository.add_mod(mod_entry);
      }
    }

    log::info!("All mods reordered successfully");
    Ok(())
  }

  /// Reorder mods based on their remote IDs and current VPK files
  pub fn reorder_mods_by_remote_id(
    &mut self,
    mod_order_data: Vec<(String, Vec<String>, u32)>, // (remote_id, current_vpks, order)
    profile_folder: Option<String>,
  ) -> Result<Vec<(String, Vec<String>)>, Error> {
    let addons_path = self.get_addons_path(profile_folder.as_ref())?;
    let mods_store = self.get_mods_store_path()?;

    log::info!(
      "Reordering mods by remote ID for {} mods in profile: {:?}",
      mod_order_data.len(),
      profile_folder
    );

    // Sort by order
    let mut sorted_data = mod_order_data;
    sorted_data.sort_by_key(|(_, _, order)| *order);

    for (i, (remote_id, _vpks, order)) in sorted_data.iter().enumerate() {
      log::info!("Position {i}: mod {remote_id} (order {order})");
    }

    // Build store-based mapping
    let mod_store_mapping: Vec<(String, PathBuf)> = sorted_data
      .iter()
      .map(|(remote_id, _, _)| {
        (
          remote_id.clone(),
          mods_store.join(remote_id).join("files"),
        )
      })
      .collect();

    let updated_mappings = self
      .vpk_manager
      .reorder_vpks_from_store(&mod_store_mapping, &addons_path)?;

    for (remote_id, new_vpks) in &updated_mappings {
      if let Some(mut mod_entry) = self.mod_repository.remove_mod(remote_id) {
        mod_entry.installed_vpks = new_vpks.clone();
        self.mod_repository.add_mod(mod_entry);
      }
    }

    log::info!("Mod reordering by remote ID completed successfully");
    Ok(updated_mappings)
  }

  /// Reorder mods based on the specified order
  pub fn reorder_mods(
    &mut self,
    mod_order_data: Vec<(String, u32)>,
    profile_folder: Option<String>,
  ) -> Result<Vec<Mod>, Error> {
    let addons_path = self.get_addons_path(profile_folder.as_ref())?;
    let mods_store = self.get_mods_store_path()?;

    log::info!(
      "Reordering {} mods for profile: {profile_folder:?}",
      mod_order_data.len()
    );

    let mut sorted_order = mod_order_data;
    sorted_order.sort_by_key(|(_, order)| *order);

    let mut mod_store_mapping = Vec::new();
    let mut updated_mods = Vec::new();

    for (mod_id, new_order) in sorted_order {
      if let Some(mut deadlock_mod) = self.mod_repository.remove_mod(&mod_id) {
        deadlock_mod.install_order = Some(new_order);
        mod_store_mapping.push((mod_id.clone(), mods_store.join(&mod_id).join("files")));
        updated_mods.push(deadlock_mod);
      } else {
        log::warn!("Mod not found in repository: {mod_id}");
      }
    }

    let updated_vpk_mappings = self
      .vpk_manager
      .reorder_vpks_from_store(&mod_store_mapping, &addons_path)?;

    let mut result_mods = Vec::new();
    for (mut deadlock_mod, (_, new_vpk_names)) in updated_mods.into_iter().zip(updated_vpk_mappings)
    {
      deadlock_mod.installed_vpks = new_vpk_names;
      self.mod_repository.add_mod(deadlock_mod.clone());
      result_mods.push(deadlock_mod);
    }

    log::info!("Successfully reordered {} mods", result_mods.len());
    Ok(result_mods)
  }

  pub fn clear_mods(&mut self, profile_folder: Option<String>) -> Result<(), Error> {
    let game_path = self
      .steam_manager
      .get_game_path()
      .ok_or(Error::GamePathNotSet)?;

    let addons_path = if let Some(ref folder) = profile_folder {
      game_path
        .join("game")
        .join("citadel")
        .join("addons")
        .join(folder)
    } else {
      game_path.join("game").join("citadel").join("addons")
    };

    self.vpk_manager.clear_all_vpks(&addons_path)?;
    Ok(())
  }

  pub fn open_mods_folder(&self, profile_folder: Option<String>) -> Result<(), Error> {
    let game_path = self
      .steam_manager
      .get_game_path()
      .ok_or(Error::GamePathNotSet)?;

    let addons_path = if let Some(ref folder) = profile_folder {
      game_path
        .join("game")
        .join("citadel")
        .join("addons")
        .join(folder)
    } else {
      game_path.join("game").join("citadel").join("addons")
    };

    self
      .filesystem
      .open_folder(addons_path.to_string_lossy().as_ref())
  }

  pub fn open_game_folder(&self) -> Result<(), Error> {
    let game_path = self
      .steam_manager
      .get_game_path()
      .ok_or(Error::GamePathNotSet)?;
    self
      .filesystem
      .open_folder(game_path.to_string_lossy().as_ref())
  }

  pub fn open_mods_data_folder(&self) -> Result<(), Error> {
    let mods_path = self.get_mods_store_path()?;
    self.filesystem.create_directories(&mods_path)?;
    self
      .filesystem
      .open_folder(mods_path.to_string_lossy().as_ref())
  }

  pub fn clear_download_cache(&self) -> Result<u64, Error> {
    let mods_path = self.get_mods_store_path()?;
    if !mods_path.exists() {
      return Ok(0);
    }

    // Collect IDs of mods that are currently installed (have enabled VPKs)
    let installed_mod_ids: std::collections::HashSet<String> = self
      .mod_repository
      .get_all_mods()
      .filter(|m| !m.installed_vpks.is_empty())
      .map(|m| m.id.clone())
      .collect();

    let mut freed = 0u64;
    for entry in std::fs::read_dir(&mods_path)? {
      let entry = entry?;
      let path = entry.path();
      if !path.is_dir() {
        continue;
      }
      let dir_name = entry.file_name().to_string_lossy().to_string();

      // Skip local mods entirely
      if dir_name.starts_with("local-") {
        continue;
      }

      if installed_mod_ids.contains(&dir_name) {
        // Mod is installed: only delete extracted/ and archive files, preserve files/
        let extracted_dir = path.join("extracted");
        if extracted_dir.exists() {
          freed += dir_size(&extracted_dir);
          self.filesystem.remove_directory_recursive(&extracted_dir)?;
        }
        // Delete archive files (zip, rar, 7z, etc.) but not the files/ directory
        for file_entry in std::fs::read_dir(&path)? {
          let file_entry = file_entry?;
          let file_path = file_entry.path();
          if file_path.is_file() {
            freed += file_path.metadata().map(|m| m.len()).unwrap_or(0);
            self.filesystem.remove_file(&file_path)?;
          }
        }
      } else {
        // Mod not installed: safe to delete entirely
        freed += dir_size(&path);
        self.filesystem.remove_directory_recursive(&path)?;
      }
    }

    log::info!("Cleared download cache: {freed} bytes freed");
    Ok(freed)
  }

  pub fn clear_all_mods_data(&self) -> Result<u64, Error> {
    let mods_path = self.get_mods_store_path()?;
    if !mods_path.exists() {
      return Ok(0);
    }

    let size = dir_size(&mods_path);
    self.filesystem.remove_directory_recursive(&mods_path)?;
    self.filesystem.create_directories(&mods_path)?;
    log::info!("Cleared all mods data: {size} bytes freed");
    Ok(size)
  }

  /// Get a reference to the steam manager
  pub fn get_steam_manager(&self) -> &SteamManager {
    &self.steam_manager
  }

  /// Get a reference to the config manager
  pub fn get_config_manager(&self) -> &GameConfigManager {
    &self.config_manager
  }

  /// Get a mutable reference to the config manager
  pub fn get_config_manager_mut(&mut self) -> &mut GameConfigManager {
    &mut self.config_manager
  }

  /// Get a reference to the mod repository
  pub fn get_mod_repository(&self) -> &ModRepository {
    &self.mod_repository
  }

  /// Get a mutable reference to the mod repository
  pub fn get_mod_repository_mut(&mut self) -> &mut ModRepository {
    &mut self.mod_repository
  }

  pub fn set_app_handle(&mut self, app_handle: tauri::AppHandle) {
    self.app_handle = Some(app_handle);
  }

  pub fn get_mods_store_path(&self) -> Result<std::path::PathBuf, Error> {
    let app_handle = self
      .app_handle
      .as_ref()
      .ok_or(Error::AppHandleNotInitialized)?;
    let app_local_data_dir = app_handle
      .path()
      .app_local_data_dir()
      .map_err(Error::Tauri)?;
    Ok(app_local_data_dir.join("mods"))
  }

  /// Replace VPK files for a mod: updates store files, then re-links if enabled
  pub fn replace_mod_vpks(
    &mut self,
    mod_id: String,
    source_vpk_paths: Vec<std::path::PathBuf>,
    installed_vpks_from_frontend: Vec<String>,
    profile_folder: Option<String>,
  ) -> Result<(), Error> {
    log::info!("Replacing VPK files for mod: {mod_id} (profile: {profile_folder:?})");

    let addons_path = self.get_addons_path(profile_folder.as_ref())?;
    let store_files_dir = self.get_mod_files_dir(&mod_id)?;

    let installed_vpks = if !installed_vpks_from_frontend.is_empty() {
      installed_vpks_from_frontend
    } else if let Some(mod_info) = self.mod_repository.get_mod(&mod_id) {
      mod_info.installed_vpks.clone()
    } else {
      Vec::new()
    };

    let new_vpks = self.vpk_manager.replace_vpks_with_store(
      &store_files_dir,
      &addons_path,
      &source_vpk_paths,
      &installed_vpks,
    )?;

    // Update repository with new VPK names if mod was enabled
    if !new_vpks.is_empty() {
      if let Some(mut mod_entry) = self.mod_repository.get_mod(&mod_id).cloned() {
        mod_entry.installed_vpks = new_vpks;
        let original_names: Vec<String> = self
          .filesystem
          .get_files_with_extension(&store_files_dir, "vpk")?
          .iter()
          .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(String::from))
          .collect();
        mod_entry.original_vpk_names = original_names;
        self.mod_repository.add_mod(mod_entry);
      }
    }

    log::info!("Successfully replaced VPK files for mod: {mod_id}");
    Ok(())
  }

  /// Validate and canonicalize a path to ensure it's within the allowed mods directory
  fn validate_path_within_mods_root(&self, path: &PathBuf) -> Result<PathBuf, Error> {
    let mods_root = self.get_mods_store_path()?;
    self.filesystem.create_directories(&mods_root)?;
    let canonical_mods_root = mods_root
      .canonicalize()
      .map_err(|_| Error::UnauthorizedPath("Unable to resolve mods directory".to_string()))?;

    // Canonicalize the requested path
    let canonical_path = if path.is_absolute() {
      path.canonicalize().map_err(|_| {
        Error::UnauthorizedPath(format!("Unable to resolve path: {}", path.display()))
      })?
    } else {
      // For relative paths, resolve them relative to the mods root
      mods_root.join(path).canonicalize().map_err(|_| {
        Error::UnauthorizedPath(format!(
          "Unable to resolve relative path: {}",
          path.display()
        ))
      })?
    };

    // Verify the canonicalized path is within the mods root
    if !canonical_path.starts_with(&canonical_mods_root) {
      return Err(Error::UnauthorizedPath(format!(
        "Path '{}' is outside the allowed mods directory '{}'",
        canonical_path.display(),
        canonical_mods_root.display()
      )));
    }

    Ok(canonical_path)
  }

  /// Public method to validate extract target paths for use by commands
  pub fn validate_extract_target_path(&self, path: &PathBuf) -> Result<PathBuf, Error> {
    self.validate_path_within_mods_root(path)
  }

  /// Validate and resolve a mod folder path, rejecting path traversal in mod_id.
  pub fn get_validated_mod_folder_path(&self, mod_id: &str) -> Result<PathBuf, Error> {
    if mod_id.contains("..") || mod_id.contains('/') || mod_id.contains('\\') {
      return Err(Error::InvalidInput(
        "Invalid mod ID: path traversal not allowed".to_string(),
      ));
    }
    let mods_root = self.get_mods_store_path()?;
    let mod_folder = mods_root.join(mod_id);
    if mod_folder.exists() {
      self.validate_path_within_mods_root(&mod_folder)
    } else {
      Ok(mod_folder)
    }
  }

  /// Remove a mod folder from the filesystem
  pub fn remove_mod_folder(&self, mod_path: &PathBuf) -> Result<(), Error> {
    log::info!("Removing mod folder: {mod_path:?}");

    // Validate and canonicalize the path to ensure it's within the mods directory
    let validated_path = self.validate_path_within_mods_root(mod_path)?;

    if !validated_path.exists() {
      log::warn!("Mod folder does not exist: {validated_path:?}");
      return Ok(());
    }

    self
      .filesystem
      .remove_directory_recursive(&validated_path)?;
    log::info!("Successfully removed mod folder: {validated_path:?}");
    Ok(())
  }

  pub fn get_addons_backup_manager(&mut self) -> &mut AddonsBackupManager {
    if let Some(game_path) = self.steam_manager.get_game_path() {
      self.addons_backup_manager.set_game_path(game_path.clone());
    }
    &mut self.addons_backup_manager
  }

  pub fn set_backup_manager_app_handle(&mut self, app_handle: tauri::AppHandle) {
    self.addons_backup_manager.set_app_handle(app_handle);
  }

  pub fn open_addons_backups_folder(&mut self) -> Result<(), Error> {
    let backup_manager = self.get_addons_backup_manager();
    let backup_dir = backup_manager.get_backup_directory()?;

    self.filesystem.create_directories(&backup_dir)?;

    self
      .filesystem
      .open_folder(backup_dir.to_string_lossy().as_ref())
  }

  pub fn get_autoexec_manager(&self) -> &AutoexecManager {
    &self.autoexec_manager
  }

  /// Recover missing VPK links in addons by re-creating them from the mod store.
  /// Returns a list of (mod_id, new_vpk_names) for each recovered mod.
  pub fn recover_mod_links(
    &mut self,
    profile_folder: Option<String>,
  ) -> Result<Vec<(String, Vec<String>)>, Error> {
    let addons_path = self.get_addons_path(profile_folder.as_ref())?;
    let mods_store = self.get_mods_store_path()?;

    log::info!("Starting mod link recovery for profile: {profile_folder:?}");

    let mut recovered = Vec::new();
    let enabled_mods: Vec<Mod> = self
      .mod_repository
      .get_all_mods()
      .filter(|m| !m.installed_vpks.is_empty())
      .cloned()
      .collect();

    for mod_entry in enabled_mods {
      let store_dir = mods_store.join(&mod_entry.id).join("files");
      if !store_dir.exists() {
        log::warn!(
          "Store files missing for enabled mod {}, cannot recover",
          mod_entry.id
        );
        continue;
      }

      // Check if any addons links are missing
      let needs_recovery = mod_entry
        .installed_vpks
        .iter()
        .any(|vpk_name| !addons_path.join(vpk_name).exists());

      if !needs_recovery {
        continue;
      }

      log::info!("Recovering links for mod: {}", mod_entry.id);

      // Remove any stale links
      for vpk_name in &mod_entry.installed_vpks {
        let p = addons_path.join(vpk_name);
        if p.exists() {
          std::fs::remove_file(&p).ok();
        }
      }

      // Re-link from store
      let new_vpks = self
        .vpk_manager
        .link_vpks_to_addons(&store_dir, &addons_path)?;

      // Update repository
      if let Some(mut m) = self.mod_repository.remove_mod(&mod_entry.id) {
        m.installed_vpks = new_vpks.clone();
        self.mod_repository.add_mod(m);
      }

      recovered.push((mod_entry.id.clone(), new_vpks));
      log::info!("Recovered mod: {}", mod_entry.id);
    }

    log::info!("Recovery complete: {} mods recovered", recovered.len());
    Ok(recovered)
  }
}

impl Default for ModManager {
  fn default() -> Self {
    Self::new()
  }
}

fn dir_size(path: &std::path::Path) -> u64 {
  std::fs::read_dir(path)
    .map(|entries| {
      entries
        .filter_map(|e| e.ok())
        .map(|e| {
          let meta = e.metadata().ok();
          if e.path().is_dir() {
            dir_size(&e.path())
          } else {
            meta.map_or(0, |m| m.len())
          }
        })
        .sum()
    })
    .unwrap_or(0)
}
