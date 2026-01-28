/*
 * Copyright (C) 2023 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

package version

const (
	//TODO: manually update
	VERSION = "0.1.0"

	//TODO: manually update
	API_VERSION = "0.1.0"
)

var (
	// GITCommit overwritten automatically by the build
	GITCOMMIT = "HEAD"

	// Built time overwritten automatically by the build
	BUILDTIME = "NOW"

	// Built hostname overwritten automatically by build
	HOSTNAME = "LOCALHOST"

	// Built branch overwritten automatically by build
	BRANCH = "MAIN"
)
