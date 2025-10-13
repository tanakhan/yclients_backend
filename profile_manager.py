import json
import os
from typing import Dict, Any, Optional


class ProfileManager:
    """Manager for handling multiple company profiles"""

    def __init__(self, profiles_file: str = None):
        # Use absolute path relative to this file if not specified
        if profiles_file is None:
            profiles_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "profiles.json")
        self.profiles_file = profiles_file
        self.profiles = {}
        self.default_profile = None
        self.load_profiles()

    def load_profiles(self):
        """Load profiles from JSON file"""
        try:
            if os.path.exists(self.profiles_file):
                with open(self.profiles_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Handle both old format (with profiles key) and new format (array)
                    if isinstance(data, list):
                        # New format: array of profiles
                        self.profiles = {profile['name']: profile for profile in data}
                        self.default_profile = data[0]['name'] if data else None
                    else:
                        # Old format: object with profiles key
                        self.profiles = data.get('profiles', {})
                        self.default_profile = data.get('default_profile')
                print(f"Loaded {len(self.profiles)} profiles from {self.profiles_file}")
            else:
                print(f"Profiles file {self.profiles_file} not found, using empty profiles")
        except Exception as e:
            print(f"Error loading profiles: {e}")
            self.profiles = {}
            self.default_profile = None

    def save_profiles(self):
        """Save profiles to JSON file"""
        try:
            # Save in new simplified format (array)
            profiles_list = list(self.profiles.values())
            with open(self.profiles_file, 'w', encoding='utf-8') as f:
                json.dump(profiles_list, f, indent=2, ensure_ascii=False)
            print(f"Saved {len(self.profiles)} profiles to {self.profiles_file}")
        except Exception as e:
            print(f"Error saving profiles: {e}")

    def get_profile(self, profile_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get a specific profile or the default one"""
        if profile_name is None:
            profile_name = self.default_profile

        if profile_name and profile_name in self.profiles:
            return self.profiles[profile_name]
        return None

    def get_all_profiles(self) -> Dict[str, Dict[str, Any]]:
        """Get all profiles"""
        return self.profiles

    def add_profile(self, profile_name: str, profile_data: Dict[str, Any]):
        """Add a new profile"""
        self.profiles[profile_name] = profile_data
        if self.default_profile is None:
            self.default_profile = profile_name
        self.save_profiles()

    def update_profile(self, profile_name: str, profile_data: Dict[str, Any]):
        """Update an existing profile"""
        if profile_name in self.profiles:
            self.profiles[profile_name].update(profile_data)
            self.save_profiles()
        else:
            raise ValueError(f"Profile {profile_name} not found")

    def delete_profile(self, profile_name: str):
        """Delete a profile"""
        if profile_name in self.profiles:
            del self.profiles[profile_name]
            if self.default_profile == profile_name:
                self.default_profile = next(iter(self.profiles.keys())) if self.profiles else None
            self.save_profiles()
        else:
            raise ValueError(f"Profile {profile_name} not found")

    def get_first_profile_name(self):
        """Get the name of the first profile (for default selection)"""
        if self.profiles:
            return next(iter(self.profiles.keys()))
        return None

    def get_proxy_settings(self, profile_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get proxy settings for a profile"""
        profile = self.get_profile(profile_name)
        if profile and profile.get('proxy', {}).get('use_proxy', False):
            return profile['proxy']
        return None


if __name__ == "__main__":
    # Test the profile manager
    pm = ProfileManager()

    # Print loaded profiles
    print("Available profiles:")
    for name, profile in pm.get_all_profiles().items():
        print(f"- {name}: {profile.get('name', 'Unnamed')}")

    # Print default profile
    default = pm.get_profile()
    if default:
        print(f"\nDefault profile: {pm.default_profile}")
        print(f"Profile data: {json.dumps(default, indent=2, ensure_ascii=False)}")